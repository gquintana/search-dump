package com.github.gquintana.searchdump;

import com.github.gquintana.searchdump.core.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchHelper<P extends SearchDocumentPartition> {

    private final String index;

    public SearchHelper(String index) {
        this.index = index;
    }

    public void create(SearchWriter writer) {
        writer.createIndex(new SearchIndex(index,
                Map.of("index", Map.of(
                        "number_of_shards", 2,
                        "number_of_replicas", 0)),
                Map.of("properties", Map.of(
                        "id", Map.of("type", "keyword"),
                        "name", Map.of("type", "text"),
                        "age", Map.of("type", "integer")
                )),
                Map.of("test", Map.of())
        ));
    }

    public void createAndFill(SearchWriter writer) {
        createAndFill(writer, 15);
    }

    public void createAndFill(SearchWriter writer, int docCount) {
        create(writer);
        try (SearchDocumentWriter docWriter = writer.writeDocuments(index)) {
            for (int i = 0; i < docCount; i++) {
                String id = String.format("id-%02d", i);
                docWriter.write(new SearchDocument(index, id,
                        Map.of("id", id, "age", i * 2, "name", "Name " + i)));
            }
        }

    }

    public void readAndCheck(SearchReader<P> reader) {
        readAndCheck(reader, 15);
    }

    public void readAndCheck(SearchReader<P> reader, int docCount) {
        checkIndex(reader);
        try (SearchDocumentReader docReader = reader.readDocuments(this.index)) {
            List<SearchDocument> docs = new ArrayList<>();
            docReader.forEachRemaining(docs::add);
            checkDocuments(docs, docCount);
        }
    }

    public void partitionedReadAndCheck(SearchReader<P> reader) {
        partitionedReadAndCheck(reader, 2, 15);
    }

    public void partitionedReadAndCheck(SearchReader<P> reader, int partitionCount, int docCount) {
        checkIndex(reader);
        List<SearchDocument> docs = new ArrayList<>();
        for (P partition : reader.splitDocuments(this.index, partitionCount)) {
            try (SearchDocumentReader docReader = reader.readDocuments(partition)) {
                docReader.forEachRemaining(docs::add);
            }
        }
        checkDocuments(docs, docCount);
    }

    private void checkDocuments(List<SearchDocument> docs, int docCount) {
        docs.sort(Comparator.comparing(SearchDocument::id));
        assertEquals(docCount, docs.size());
        for (int i = 0; i < docs.size(); i++) {
            SearchDocument doc = docs.get(i);
            assertEquals(String.format("id-%02d", i), doc.id());
            assertEquals(this.index, doc.index());
            assertEquals(i * 2, doc.source().get("age"));
            assertEquals("Name " + i, doc.source().get("name"));
        }
    }

    private void checkIndex(SearchReader<P> reader) {
        List<String> foundIndices = reader.listIndices(List.of(index));
        assertTrue(foundIndices.contains(index));
        SearchIndex index = reader.getIndex(this.index);
        assertEquals(this.index, index.name());
        @SuppressWarnings("unchecked")
        Map<String, Object> indexSettings = (Map<String, Object>) index.settings().get("index");
        assertEquals(2, indexSettings.keySet().stream().filter(k -> k.contains("number")).count());
        @SuppressWarnings("unchecked")
        Map<String, Object> indexMappings = (Map<String, Object>) index.mappings().get("properties");
        assertEquals(3, indexMappings.size());
        assertEquals(1, index.aliases().size());
    }

    public void copy(SearchReader<P> reader, SearchWriter writer) {
        copy(reader, writer, 1);
    }

    public void copy(SearchReader<P> reader, SearchWriter writer, int partitionCount) {
        new SearchCopier<P>(reader, writer, false, false, partitionCount).copy(this.index);
    }

    public void createList(SearchWriter port) {
        for (int i = 0; i < 3; i++) {
            SearchHelper<P> helperi = new SearchHelper<>(index + "-" + i);
            helperi.create(port);
        }
    }

    public void listAndCheck(SearchReader<P> port) {
        List<String> indices = port.listIndices(List.of(index + "-*"));
        assertEquals(3, indices.size());
    }
}
