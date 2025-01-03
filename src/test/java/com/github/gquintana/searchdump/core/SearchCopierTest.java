package com.github.gquintana.searchdump.core;

import com.github.gquintana.searchdump.SearchHelper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SearchCopierTest {
    @Test
    void copySingle() {
        FakeSearchReader fakeReader = createFakeSearchReader("copy-1");
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        SearchCopier copier = new SearchCopier(fakeReader, fakeWriter, false, false);
        copier.copy("copy-1");
        assertTrue(fakeWriter.getIndices().containsKey("copy-1"));
        assertEquals(15, fakeWriter.getDocuments().size());
    }

    @Test
    void copyMultiple() {
        FakeSearchReader fakeReader = createFakeSearchReader("copy-1", "copy-2");
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        SearchCopier copier = new SearchCopier(fakeReader, fakeWriter, false, false);
        copier.copy(List.of("copy-*"));
        assertTrue(fakeWriter.getIndices().containsKey("copy-1"));
        assertTrue(fakeWriter.getIndices().containsKey("copy-2"));
        assertEquals(30, fakeWriter.getDocuments().size());
    }

    @Test
    void copySkipExisting() {
        FakeSearchReader fakeReader = createFakeSearchReader("copy-1");
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        SearchCopier copier = new SearchCopier(fakeReader, fakeWriter, false, true);
        copier.copy("copy-1");
        copier.copy("copy-1");
        assertTrue(fakeWriter.getIndices().containsKey("copy-1"));
        assertEquals(15, fakeWriter.getDocuments().size());
    }

    @Test
    void copyDontSkipExisting() {
        FakeSearchReader fakeReader = createFakeSearchReader("copy-1");
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        SearchCopier copier = new SearchCopier(fakeReader, fakeWriter, false, false);
        copier.copy("copy-1");
        copier.copy("copy-1");
        assertTrue(fakeWriter.getIndices().containsKey("copy-1"));
        assertEquals(30, fakeWriter.getDocuments().size());
    }

    @Test
    void copySkipFailed() {
        FakeSearchReader fakeReader = createFakeSearchReader("copy-1", "copy-2");
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        fakeWriter.failAt("copy-1", 10);
        SearchCopier copier = new SearchCopier(fakeReader, fakeWriter, true, false);
        copier.copy(List.of("copy-*"));
        assertTrue(fakeWriter.getIndices().containsKey("copy-1"));
        assertTrue(fakeWriter.getIndices().containsKey("copy-2"));
        assertEquals(26, fakeWriter.getDocuments().size());
    }
    @Test
    void copyDontSkipFailed() {
        FakeSearchReader fakeReader = createFakeSearchReader("copy-1", "copy-2");
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        fakeWriter.failAt("copy-1", 10);
        SearchCopier copier = new SearchCopier(fakeReader, fakeWriter, false, false);
        try {
            copier.copy(List.of("copy-*"));
        } catch (IllegalStateException e) {
        }
        assertTrue(fakeWriter.getIndices().containsKey("copy-1"));
        assertFalse(fakeWriter.getIndices().containsKey("copy-2"));
        assertEquals(11, fakeWriter.getDocuments().size());
    }
    private static @NotNull FakeSearchReader createFakeSearchReader(String ... indices) {
        FakeSearchWriter fakeWriter = new FakeSearchWriter();
        for (String index : indices) {
            SearchHelper helper = new SearchHelper(index);
            helper.createAndFill(fakeWriter);
        }
        return fakeWriter.toReader();
    }

}