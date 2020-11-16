/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;

/**
 * <p>
 * The principle idea comes from Guava. However, the calculation of Guava is
 * rate-based, which means that we need to translate rate to QPS.
 * </p>
 * <p>
 * 思想来自于Guava 不同的是将速率转换为qps
 *
 * <p>
 * Requests arriving at the pulse may drag down long idle systems even though it
 * has a much larger handling capability in stable period. It usually happens in
 * scenarios that require extra time for initialization, e.g. DB establishes a connection,
 * connects to a remote service, and so on. That’s why we need “warm up”.
 * </p>
 * 即使在稳定时间内*具有更大的处理能力，到达脉冲的请求也可能拖累长时间闲置的系统。
 * 它通常发生在*需要额外时间进行初始化的情况下，例如DB建立连接，*连接到远程服务，依此类推。这就是为什么我们需要“热身”
 * <p>
 * Sentinel's "warm-up" implementation is based on the Guava's algorithm.
 * However, Guava’s implementation focuses on adjusting the request interval,
 * which is similar to leaky bucket. Sentinel pays more attention to
 * controlling the count of incoming requests per second without calculating its interval,
 * which resembles token bucket algorithm.
 * </p>
 *  Sentinel的“热身”实现是基于guaua算法的。
 *  实现方式着重于调整请求间隔*与漏斗类似。
 *  Sentinel更注重*在不计算其间隔的情况下控制每秒传入请求的数量
 *  与令牌桶算法类似。
 * <p>
 * The remaining tokens in the bucket is used to measure the system utility.
 * Suppose a system can handle b requests per second. Every second b tokens will
 * be added into the bucket until the bucket is full. And when system processes
 * a request, it takes a token from the bucket. The more tokens left in the
 * bucket, the lower the utilization of the system; when the token in the token
 * bucket is above a certain threshold, we call it in a "saturation" state.
 * </p>
 *  存储桶中剩余的令牌用于测量系统实用程序。
 *  假设系统每秒可以处理b个请求。
 *  每秒钟b令牌将*添加到存储桶中，直到存储桶已满。当系统处理请求时，它会从存储桶中获取令牌。
 *  桶中剩余的令牌越多，系统的利用率越低；当令牌
 *  存储桶中的令牌高于某个阈值时，我们将其称为“饱和”状态。
 * <p>
 * Base on Guava’s theory, there is a linear equation we can write this in the
 * form y = m * x + b where y (a.k.a y(x)), or qps(q)), is our expected QPS
 * given a saturated period (e.g. 3 minutes in), m is the rate of change from
 * our cold (minimum) rate to our stable (maximum) rate, x (or q) is the
 * occupied token.
 * 有一个线性方程可以写成*形式y = m * x + b其中y（aka y（x））或qps（q））是我们期望的QPS
 * *给定饱和周期（例如3分钟），
 * m是从*我们的冷（最小）速率到我们的稳定（最大）速率的变化速率，
 * x（或q）是*占用令牌。
 * </p>
 *
 * @author jialiang.linjl
 */
public class WarmUpController implements TrafficShapingController {

    protected double count;//数量
    private int coldFactor;//冷因子
    protected int warningToken = 0; //告警toke
    private int maxToken;           //最大token
    protected double slope; //斜率

    protected AtomicLong storedTokens = new AtomicLong(0);
    protected AtomicLong lastFilledTime = new AtomicLong(0);

    public WarmUpController(double count, int warmUpPeriodInSec, int coldFactor) {
        construct(count, warmUpPeriodInSec, coldFactor);
    }

    public WarmUpController(double count, int warmUpPeriodInSec) {
        construct(count, warmUpPeriodInSec, 3);
    }

    /**
     * 连接: https://blog.csdn.net/prestigeding/article/details/105341098
     * @param count
     * @param warmUpPeriodInSec
     * @param coldFactor
     */
    private void construct(double count, int warmUpPeriodInSec, int coldFactor) {

        if (coldFactor <= 1) {
            throw new IllegalArgumentException("Cold factor should be larger than 1");
        }
        //限流规则配置的阔值，例如是按 TPS 类型来限流，如果限制为100tps，则该值为100。
        this.count = count;
        //冷冻因子
        this.coldFactor = coldFactor;

        // thresholdPermits = 0.5 * warmupPeriod / stableInterval.
        // warningToken = 100;
        //对比guaua thresholdPermits = 0.5 * warmupPeriod / stableInterval.
        /**
         * 具体请看正方形和梯形面积图
         * 计算 warningToken 的值，
         * 与 Guava 中的 RateLimiter 中的 thresholdPermits 的计算算法公式相同，
         * thresholdPermits = 0.5 * warmupPeriod / stableInterval，在Sentienl 中，
         * 而 stableInteral = 1 / count，
         * thresholdPermits 表达式中的 0.5 就是因为 codeFactor 为3，
         * 因为 warm up period与 stable 面积之比等于 (coldIntervalMicros - stableIntervalMicros ) 与 stableIntervalMicros 的比值，
         * 这个比值又等于 coldIntervalMicros / stableIntervalMicros - stableIntervalMicros / stableIntervalMicros
         * 等于 coldFactor - 1。
         */
        warningToken = (int) (warmUpPeriodInSec * count) / (coldFactor - 1);
        // / maxPermits = thresholdPermits + 2 * warmupPeriod /
        // (stableInterval + coldInterval)
        // maxToken = 200
        //同理推导出最大的token
        maxToken = warningToken + (int) (2 * warmUpPeriodInSec * count / (1.0 + coldFactor));

        // slope
        // slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits
        // - thresholdPermits);
        //计算出增长的斜率
        slope = (coldFactor - 1.0) / count / (maxToken - warningToken);

    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        //已经通过的qps当前节点
        long passQps = (long) node.passQps();
        //最近的一个时间窗口通过的qps
        long previousQps = (long) node.previousPassQps();
        //调用 syncToken 更新 storedTokens 与 lastFilledTime 的值，即按照令牌发放速率发送指定令牌
        syncToken(previousQps);

        // 开始计算它的斜率
        // 如果进入了警戒线，开始调整他的qps
        long restToken = storedTokens.get();
        //第一次请求 max token 肯定是 大于warningToken
        if (restToken >= warningToken) {
            //超过warn的token
            long aboveToken = restToken - warningToken;
            // 消耗的速度要比warning快，但是要比慢
            // current interval = restToken*slope+1/count
            //如果当前存储的许可大于warningToken的处理逻辑，
            // 主要是在预热阶段允许通过的速率会比限流规则设定的速率要低，
            // 判断是否通过的依据就是当前通过的TPS与申请的许可数是否小于当前的速率（这个值加入斜率，即在预热期间，速率是慢慢达到设定速率的。
            double warningQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));
            if (passQps + acquireCount <= warningQps) {
                return true;
            }
        } else {
            if (passQps + acquireCount <= count) {
                return true;
            }
        }

        return false;
    }

    protected void syncToken(long passQps) {
        //获取当前时间
        long currentTime = TimeUtil.currentTimeMillis();
        //转为秒
        currentTime = currentTime - currentTime % 1000;
        long oldLastFillTime = lastFilledTime.get();
        if (currentTime <= oldLastFillTime) {//如果当前时间小于等于上次发放许可的时间，则跳过，无法发放令牌，即每秒发放一次令牌。
            return;
        }
        //剩余令牌数
        long oldValue = storedTokens.get();
        //第一次为最大的maxToken
        long newValue = coolDownTokens(currentTime, passQps);
        //更新剩余令牌数 ，即生成的许可后要减去上一秒通过的令牌。
        if (storedTokens.compareAndSet(oldValue, newValue)) {
            long currentValue = storedTokens.addAndGet(0 - passQps);
            if (currentValue < 0) {
                storedTokens.set(0L);
            }
            lastFilledTime.set(currentTime);
        }

    }

    private long coolDownTokens(long currentTime, long passQps) {
        //当前剩余的token数
        long oldValue = storedTokens.get();
        long newValue = oldValue;

        // 添加令牌的判断前提条件:
        // 当令牌的消耗程度远远低于警戒线的时候
        if (oldValue < warningToken) {
            newValue = (long) (oldValue + (currentTime - lastFilledTime.get()) * count / 1000); //正常速率
        } else if (oldValue > warningToken) {//
            if (passQps < (int) count / coldFactor) {//如果当前剩余的 token 大于警戒线但前一秒的QPS小于 (count 与 冷却因子的比)，也发放许可（这里我不是太明白其用意）。
                newValue = (long) (oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
            }
        }
        //这里是关键点，第一次运行，由于 lastFilledTime 等于0，这里将返回的是 maxToken，故这里一开始的许可就会超过 warningToken，启动预热机制，进行速率限制。
        return Math.min(newValue, maxToken);
    }

    public static void main(String[] args) {
        long currentTime = TimeUtil.currentTimeMillis();
        //
        currentTime = currentTime - currentTime % 1000;
        System.out.println(currentTime);
    }

}
