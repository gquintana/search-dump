package com.github.gquintana.searchdump;

import com.github.gquintana.searchdump.core.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchPortHelper {

    private final String index;

    public SearchPortHelper(String index) {
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

    public void readAndCheck(SearchReader reader) {
        readAndCheck(reader, 15);
    }

    public void readAndCheck(SearchReader reader, int docCount) {
        List<String> foundIndices = reader.listIndices(List.of(index));
        assertTrue(foundIndices.contains(index));
        SearchIndex index = reader.getIndex(this.index);
        assertEquals(this.index, index.name());
        assertEquals(2, ((Map<String, Object>) index.settings().get("index")).keySet().stream().filter(k -> k.contains("number")).count());
        assertEquals(3, ((Map<String, Object>) index.mappings().get("properties")).size());
        assertEquals(1, index.aliases().size());
        try (SearchDocumentReader docReader = reader.readDocuments(this.index)) {
            List<SearchDocument> docs = new ArrayList<>();
            docReader.forEachRemaining(docs::add);
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
    }

    public void copy(SearchReader reader, SearchWriter writer) {
        new SearchCopier(reader, writer, false, false).copy(this.index);
    }

    public void createList(SearchWriter port) {
        for (int i = 0; i < 3; i++) {
            SearchPortHelper helperi = new SearchPortHelper(index + "-" + i);
            helperi.create(port);
        }
    }

    public void listAndCheck(SearchReader port) {
        List<String> indices = port.listIndices(List.of(index + "-*"));
        assertEquals(3, indices.size());
    }
}
