package com.github.gquintana.searchdump.core;

public interface SearchDocumentWriter extends QuietCloseable {
    void write(SearchDocument document);
    void flush();
}
