package com.github.gquintana.searchdump.core;

public class SearchDocumentPartition {
    private final String index;
    private final int partitionIndex;
    private final int partitionCount;

    public SearchDocumentPartition(String index, int partitionIndex, int partitionCount) {
        this.index = index;
        this.partitionIndex = partitionIndex;
        this.partitionCount = partitionCount;
    }

    public String index() {
        return index;
    }

    public int partitionIndex() {
        return partitionIndex;
    }

    public int partitionCount() {
        return partitionCount;
    }
}
