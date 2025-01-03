package com.github.gquintana.searchdump.core;

import com.github.gquintana.searchdump.SearchHelper;
import org.junit.jupiter.api.Test;

public abstract class AbstractSearchTest<W extends SearchWriter, R extends SearchReader> {
    protected abstract R createReader();

    protected abstract W createWriter();

    protected void refreshIndex(W writer, String index) {
    }

    @Test
    void writeAndRead() {
        SearchHelper helper = new SearchHelper("test-1");
        try (W writer = createWriter()) {
            helper.createAndFill(writer);
            refreshIndex(writer, "test-1");
        }
        try (R reader = createReader()) {
            helper.readAndCheck(reader);
        }
    }

    @Test
    void writeWhenNoDocuments() {
        SearchHelper helper = new SearchHelper("test-no-docs");
        try (W writer = createWriter()) {
            helper.createAndFill(writer, 0);
            refreshIndex(writer, "test-no-docs");
        }
        try (R reader = createReader()) {
            helper.readAndCheck(reader, 0);
        }
    }
    @Test
    void listIndexes() {
        SearchHelper helper = new SearchHelper("list");
        try (W writer = createWriter()) {
            helper.createList(writer);
        }
        try (R reader = createReader()) {
            helper.listAndCheck(reader);
        }
    }
    @Test
    void copyTo() {
        SearchHelper helper = new SearchHelper("test-2");
        try (W writer = createWriter()) {
            helper.createAndFill(writer);
            refreshIndex(writer, "test-2");
        }
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        try (R reader = createReader()) {
            helper.copy(reader, fakeWriter);
        }
        FakeSearchReader fakeReader = fakeWriter.toReader();
        helper.readAndCheck(fakeReader);
    }

    @Test
    void copyFrom() {
        SearchHelper helper = new SearchHelper("test-3");
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        helper.createAndFill(fakeWriter);
        FakeSearchReader fakeReader = fakeWriter.toReader();
        try(W writer = createWriter()) {
            helper.copy(fakeReader, writer);
            refreshIndex(writer, "test-3");
        }
        try (R reader = createReader()) {
            helper.readAndCheck(reader);
        }
    }

    @Test
    void copyToAndFrom() {
        SearchHelper helper = new SearchHelper("test-4");
        try (W writer = createWriter()) {
            helper.createAndFill(writer);
            refreshIndex(writer, "test-4");
        }
        // Backup
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        try (R reader = createReader()) {
            helper.copy(reader, fakeWriter);
        }
        // Delete
        deleteIndex("test-4");
        // Restore
        FakeSearchReader fakeReader = fakeWriter.toReader();
        try (W writer = createWriter()) {
            helper.copy(fakeReader, writer);
            refreshIndex(writer, "test-4");
        }
    }

    protected void deleteIndex(String index) {

    }
}
