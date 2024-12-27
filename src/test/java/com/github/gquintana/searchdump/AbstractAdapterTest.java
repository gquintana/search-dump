package com.github.gquintana.searchdump;

import com.github.gquintana.searchdump.core.SearchReader;
import com.github.gquintana.searchdump.core.SearchWriter;
import org.junit.jupiter.api.Test;

public abstract class AbstractAdapterTest<W extends SearchWriter, R extends SearchReader> {
    protected abstract R createReader();

    protected abstract W createWriter();

    protected void refreshIndex(W writer, String index) {
    }

    @Test
    void testExportImport() {
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
    void testList() {
        SearchPortHelper helper = new SearchPortHelper("list");
        try (W writer = createWriter()) {
            helper.createList(writer);
        }
        try (R reader = createReader()) {
            helper.listAndCheck(reader);
        }
    }

}
