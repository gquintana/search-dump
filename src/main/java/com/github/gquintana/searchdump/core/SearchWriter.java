package com.github.gquintana.searchdump.core;

public interface SearchWriter extends QuietCloseable {
    void createIndex(SearchIndex index);

    SearchDocumentWriter writeDocuments(String index);
}
