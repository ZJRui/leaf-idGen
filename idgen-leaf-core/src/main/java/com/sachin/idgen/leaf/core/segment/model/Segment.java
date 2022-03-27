package com.sachin.idgen.leaf.core.segment.model;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
@Data
public class Segment {

    /**
     * 当前使用的值，也就是取号值
     */
    private AtomicLong value = new  AtomicLong(0);
    /**
     * 最大可用取号值
     */
    private volatile long max;
    private volatile  int step;
    private SegmentBuffer buffer;

    public Segment(SegmentBuffer buffer) {
        this.buffer = buffer;
    }
    public long getIdle() {
        return this.getMax() - getValue().get();
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Segment(");
        sb.append("value:");
        sb.append(value);
        sb.append(",max:");
        sb.append(max);
        sb.append(",step:");
        sb.append(step);
        sb.append(")");
        return sb.toString();
    }
}
