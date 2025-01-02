package com.github.gquintana.searchdump.core;

public interface SearchWriter extends QuietCloseable {
    boolean createIndex(SearchIndex index);
    SearchDocumentWriter writeDocuments(String index);
}
