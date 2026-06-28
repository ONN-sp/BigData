package org.example.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyPuntuatedWatermarkGenerator<T> implements WatermarkGenerator<T> {
    private long maxTs;// 用来保存当前最大的事件时间

    /** The maximum out-of-orderness that this watermark generator assumes. */
    private final long delayTs;// 乱序延迟时间

    public MyPuntuatedWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE  + delayTs + 1;
    }

    /**
     * 每条数据来都调用一次：用来提取最大的事件时间，保存下来
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTs = Math.max(maxTs, eventTimestamp);
        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
    }

    /**
     * 周期性调用：发射watermark
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
}
