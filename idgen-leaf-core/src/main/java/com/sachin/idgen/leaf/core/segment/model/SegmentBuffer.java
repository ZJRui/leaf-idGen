package com.sachin.idgen.leaf.core.segment.model;


import lombok.Data;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author Sachin
 * @Date 2022/3/27
 **/
@Data
public class SegmentBuffer {

    /**
     * 业务tag
     */
    private String key;

    /**
     * 双buffer
     */
    private Segment[] segments;

    /**
     * 当前使用的Segment的index， 也就是使用是segements数组中的哪个Segment
     * 存在多线程访问，需要使用volatile
     */
    private volatile int currentPos;

    /**
     * 下一个Segment是否处于可切换状态。 为什么这个属性不放置在Segment中？
     */
    private volatile boolean nextReady;
    /**
     * 是否初始化完成
     */
    private  volatile boolean initOk;

    /**
     * 线程是否在运行中。 问题：为什么不使用volatile boolean
     */
    private final AtomicBoolean  threadRunning;

    private final ReadWriteLock lock;

    private volatile int step;
    private volatile int minStep;
    private  volatile long updateTimestamp;

    public SegmentBuffer() {
        segments = new Segment[]{new Segment(this), new Segment(this)};
        currentPos=0;
        nextReady=false;
        initOk=false;
        threadRunning = new AtomicBoolean(false);
        lock = new ReentrantReadWriteLock();
    }


    public Segment getCurrent(){
        return segments[currentPos];
    }

    public Lock rLock() {
        return lock.readLock();
    }

    public Lock wLock() {
        return lock.writeLock();
    }

    public int nextPos() {
        return (currentPos + 1) % 2;
    }

    public void switchPos() {
        currentPos = nextPos();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SegmentBuffer{");
        sb.append("key='").append(key).append('\'');
        sb.append(", segments=").append(Arrays.toString(segments));
        sb.append(", currentPos=").append(currentPos);
        sb.append(", nextReady=").append(nextReady);
        sb.append(", initOk=").append(initOk);
        sb.append(", threadRunning=").append(threadRunning);
        sb.append(", step=").append(step);
        sb.append(", minStep=").append(minStep);
        sb.append(", updateTimestamp=").append(updateTimestamp);
        sb.append('}');
        return sb.toString();
    }
}
