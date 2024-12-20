package com.github.gquintana.searchdump;

import com.github.gquintana.searchdump.core.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchPortHelper {

    private final String index;

    public SearchPortHelper(String index) {
        this.index = index;
    }

    public void createAndFill(SearchWriter port) {
        port.createIndex(new SearchIndex(index,
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
        try (SearchDocumentWriter writer = port.writeDocuments(index)) {
            for (int i = 0; i < 15; i++) {
                String id = String.format("id-%02d", i);
                writer.write(new SearchDocument(index, id,
                        Map.of("id", id, "age", i * 2, "name", "Name " + i)));
            }
        }

    }

    public void readAndCheck(SearchReader port) {
        SearchIndex index = port.getIndex(this.index);
        assertEquals(this.index, index.name());
        assertEquals(2, ((Map<String, Object>) index.settings().get("index")).keySet().stream().filter(k -> k.contains("number")).count());
        assertEquals(3, ((Map<String, Object>) index.mappings().get("properties")).size());
        assertEquals(1, index.aliases().size());
        try (SearchDocumentReader reader = port.readDocuments(this.index)) {
            List<SearchDocument> docs = new ArrayList<>();
            reader.forEachRemaining(docs::add);
            docs.sort(Comparator.comparing(SearchDocument::id));
            assertEquals(15, docs.size());
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
        new SearchCopier(reader, writer).copy(this.index);
    }
}
