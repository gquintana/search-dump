package com.github.gquintana.searchdump.core;

import java.util.*;

public class FakeSearchWriter implements SearchWriter {
    private final Map<String, SearchIndex> indices = new HashMap<>();
    private final List<SearchDocument> documents = new ArrayList<>();
    private Fail fail;

    private record Fail(String index, int pos) {

    }
    public Map<String, SearchIndex> getIndices() {
        return Collections.unmodifiableMap(indices);
    }

    public List<SearchDocument> getDocuments() {
        return Collections.unmodifiableList(documents);
    }

    @Override
    public boolean createIndex(SearchIndex index) {
        if (this.indices.containsKey(index.name())) {
            return false;
        } else {
            this.indices.put(index.name(), index);
            return true;
        }
    }

    public FakeSearchReader toReader() {
        return new FakeSearchReader(getIndices(), getDocuments());
    }

    public void failAt(String index, int pos) {
        this.fail = new Fail(index, pos);
    }

    private static class FakeSearchDocumentWriter implements SearchDocumentWriter {
        private final List<SearchDocument> documents;
        private final List<SearchDocument> bulk = new ArrayList<>();
        private final Fail fail;
        private int pos = 0;

        public FakeSearchDocumentWriter(List<SearchDocument> documents, Fail fail) {
            this.documents = documents;
            this.fail = fail;
        }

        @Override
        public void write(SearchDocument document) {
            bulk.add(document);
            if (fail != null && this.pos >= fail.pos) {
                throw new IllegalStateException("Write fail");
            }
            pos++;
        }

        @Override
        public void flush() {
            this.documents.addAll(bulk);
            this.bulk.clear();
        }

        @Override
        public void close() {
            flush();
        }
    }
    @Override
    public SearchDocumentWriter writeDocuments(String index) {
        return new FakeSearchDocumentWriter(this.documents, this.fail != null && this.fail.index.equals(index) ? this.fail : null);
    }
}
