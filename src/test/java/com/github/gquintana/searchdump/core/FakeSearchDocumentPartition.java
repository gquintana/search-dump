package com.github.gquintana.searchdump.core;

import java.util.List;

public class FakeSearchDocumentPartition extends SearchDocumentPartition {
    private final List<SearchDocument> documents;

    public FakeSearchDocumentPartition(String index, int partitionIndex, int partitionCount, List<SearchDocument> documents) {
        super(index, partitionIndex, partitionCount);
        this.documents = documents;
    }

    public List<SearchDocument> documents() {
        return documents;
    }
}
