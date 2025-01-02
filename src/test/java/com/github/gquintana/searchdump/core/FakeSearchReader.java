package com.github.gquintana.searchdump.core;

import java.util.*;
import java.util.stream.Collectors;

public class FakeSearchReader implements SearchReader {
    private final Map<String, SearchIndex> indices = new HashMap<>();
    private final List<SearchDocument> documents = new ArrayList<>();

    public FakeSearchReader() {
    }

    public FakeSearchReader(Map<String, SearchIndex> indices, List<SearchDocument> documents) {
        this.indices.putAll(indices);
        this.documents.addAll(documents);
    }

    public void addIndex(SearchIndex index) {
        this.indices.put(index.name(), index);
    }

    public void addDocument(SearchDocument document) {
        this.documents.add(document);
    }

    @Override
    public List<String> listIndices(List<String> indices) {
        MultiGlobMatcher matcher = new MultiGlobMatcher(indices);
        return this.indices.keySet().stream().filter(matcher::matches).collect(Collectors.toList());
    }

    @Override
    public SearchIndex getIndex(String name) {
        return this.indices.get(name);
    }

    private static class FakeSearchDocumentReader implements SearchDocumentReader {
        private final Iterator<SearchDocument> documents;
        public FakeSearchDocumentReader(Iterator<SearchDocument> documents) {
            this.documents = documents;
        }

        @Override
        public boolean hasNext() {
            return documents.hasNext();
        }

        @Override
        public SearchDocument next() {
            return documents.next();
        }
    }
    @Override
    public SearchDocumentReader readDocuments(String index) {
        return new FakeSearchDocumentReader(this.documents.stream().filter(d -> d.index().equals(index)).iterator());
    }
}
