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
package com.alibaba.csp.sentinel.slots.statistic.metric.occupy;

import java.util.List;

import com.alibaba.csp.sentinel.slots.statistic.MetricEvent;
import com.alibaba.csp.sentinel.slots.statistic.base.LeapArray;
import com.alibaba.csp.sentinel.slots.statistic.base.WindowWrap;
import com.alibaba.csp.sentinel.slots.statistic.data.MetricBucket;

/**
 * @author jialiang.linjl
 * @since 1.5.0
 * 所谓的 OccupiableBucketLeapArray ，
 * 实现的思想是当前抽样统计中的“令牌”已耗尽，即达到用户设定的相关指标的阔值后，
 * 可以向下一个时间窗口，即借用未来一个采样区间
 */
public class OccupiableBucketLeapArray extends LeapArray<MetricBucket> {

    private final FutureBucketLeapArray borrowArray;

    /**
     * 从构造函数可以看出，不仅创建了一个常规的 LeapArray，对应一个采集周期，还会创建一个 borrowArray ，也会包含一个采集周期。
     * @param sampleCount
     * @param intervalInMs
     */
    public OccupiableBucketLeapArray(int sampleCount, int intervalInMs) {
        // This class is the original "CombinedBucketArray".
        super(sampleCount, intervalInMs);
        this.borrowArray = new FutureBucketLeapArray(sampleCount, intervalInMs);
    }

    @Override
    public MetricBucket newEmptyBucket(long time) {
        //首先新建一个 MetricBucket。
        MetricBucket newBucket = new MetricBucket();
       //在新建的时候，如果曾经有借用过未来的滑动窗口，则将未来的滑动窗口上收集的数据 copy 到新创建的采集指标上，再返回。
        MetricBucket borrowBucket = borrowArray.getWindowValue(time);
        if (borrowBucket != null) {
            newBucket.reset(borrowBucket);
        }

        return newBucket;
    }

    /**
     * 重置窗口
     * @param w
     * @param time
     * @return
     */
    @Override
    protected WindowWrap<MetricBucket> resetWindowTo(WindowWrap<MetricBucket> w, long time) {
        // Update the start time and reset value.
        //遇到过期的滑动窗口时，需要对滑动窗口进行重置，这里的思路和 newEmptyBucket 的核心思想是一样的，即如果存在已借用的情况，在重置后需要加上在未来已使用过的许可
        w.resetTo(time);
        MetricBucket borrowBucket = borrowArray.getWindowValue(time);
        if (borrowBucket != null) {
            w.value().reset();
            w.value().addPass((int)borrowBucket.pass());
        } else {
            w.value().reset();
        }

        return w;
    }

    /**
     *
     * @return
     */
    @Override
    public long currentWaiting() {
        //当前的windowds
        borrowArray.currentWindow();
        long currentWaiting = 0;
        List<MetricBucket> list = borrowArray.values();

        for (MetricBucket window : list) {
            currentWaiting += window.pass();
        }
        return currentWaiting;
    }

    //获取借用的
    @Override
    public void addWaiting(long time, int acquireCount) {
        WindowWrap<MetricBucket> window = borrowArray.currentWindow(time);
        window.value().add(MetricEvent.PASS, acquireCount);
    }

    @Override
    public void debug(long time) {
        StringBuilder sb = new StringBuilder();
        List<WindowWrap<MetricBucket>> lists = listAll();
        sb.append("a_Thread_").append(Thread.currentThread().getId()).append(" time=").append(time).append("; ");
        for (WindowWrap<MetricBucket> window : lists) {
            sb.append(window.windowStart()).append(":").append(window.value().toString()).append(";");
        }
        sb.append("\n");

        lists = borrowArray.listAll();
        sb.append("b_Thread_").append(Thread.currentThread().getId()).append(" time=").append(time).append("; ");
        for (WindowWrap<MetricBucket> window : lists) {
            sb.append(window.windowStart()).append(":").append(window.value().toString()).append(";");
        }
        System.out.println(sb.toString());
    }
}
