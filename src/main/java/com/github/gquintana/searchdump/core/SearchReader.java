package com.github.gquintana.searchdump.core;

public interface SearchReader extends QuietCloseable {
    SearchIndex getIndex(String name);

    SearchDocumentReader readDocuments(String index);
}
