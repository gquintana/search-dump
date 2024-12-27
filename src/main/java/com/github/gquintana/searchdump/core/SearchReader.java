package com.github.gquintana.searchdump.core;

import java.util.List;

public interface SearchReader extends QuietCloseable {
    List<String> listIndices(List<String> indices);
    SearchIndex getIndex(String name);
    SearchDocumentReader readDocuments(String index);
}
