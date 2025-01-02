package com.github.gquintana.searchdump.core;

import com.github.gquintana.searchdump.SearchPortHelper;
import org.junit.jupiter.api.Test;

public abstract class AbstractAdapterTest<W extends SearchWriter, R extends SearchReader> {
    protected abstract R createReader();

    protected abstract W createWriter();

    protected void refreshIndex(W writer, String index) {
    }

    @Test
    void writeAndRead() {
        SearchPortHelper helper = new SearchPortHelper("test-1");
        try (W writer = createWriter()) {
            helper.createAndFill(writer);
            refreshIndex(writer, "test-1");
        }
        try (R reader = createReader()) {
            helper.readAndCheck(reader);
        }
    }

    @Test
    void listIndexes() {
        SearchPortHelper helper = new SearchPortHelper("list");
        try (W writer = createWriter()) {
            helper.createList(writer);
        }
        try (R reader = createReader()) {
            helper.listAndCheck(reader);
        }
    }
    @Test
    void copyTo() {
        SearchPortHelper helper = new SearchPortHelper("test-2");
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
        SearchPortHelper helper = new SearchPortHelper("test-3");
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
        SearchPortHelper helper = new SearchPortHelper("test-4");
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
