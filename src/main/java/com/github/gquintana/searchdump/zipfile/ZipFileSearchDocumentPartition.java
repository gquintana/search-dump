package com.github.gquintana.searchdump.zipfile;

import com.github.gquintana.searchdump.core.SearchDocumentPartition;

import java.util.List;
import java.util.zip.ZipEntry;

public class ZipFileSearchDocumentPartition extends SearchDocumentPartition {
    private final List<ZipEntry> zipEntries;

    public ZipFileSearchDocumentPartition(String index, int partitionIndex, int partitionCount, List<ZipEntry> zipEntries) {
        super(index, partitionIndex, partitionCount);
        this.zipEntries = zipEntries;
    }

    public List<ZipEntry> zipEntries() {
        return zipEntries;
    }
}
