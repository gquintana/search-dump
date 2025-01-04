package com.github.gquintana.searchdump.core;

import java.util.List;

public interface SearchReader<P extends SearchDocumentPartition> extends QuietCloseable {
    List<String> listIndices(List<String> indices);
    SearchIndex getIndex(String name);
    List<P> splitDocuments(String index, int partitionCount);
    SearchDocumentReader readDocuments(String index);
    SearchDocumentReader readDocuments(P partition);
}
