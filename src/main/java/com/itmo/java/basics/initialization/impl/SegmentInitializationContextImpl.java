package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.nio.file.Path;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {

    private final String segmentName;
    private final Path segmentPath;
    private final long currentSize;
    private final SegmentIndex index;
    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    /**
     * Не используйте этот конструктор. Оставлен для совместимости со старыми тестами.
     */
    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this.segmentName = segmentName;
        segmentPath = tablePath.resolve(segmentName);
        this.currentSize = currentSize;
        index = null;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath) {
        this(segmentName, tablePath.resolve(segmentName), 0, new SegmentIndex());
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }
}
