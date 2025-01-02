package com.github.gquintana.searchdump.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.json.JsonpDeserializer;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.json.jackson.JacksonJsonpParser;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchWriter implements SearchWriter, QuietCloseable {
    private final int writeBulkSize;
    private final ElasticsearchClient client;
    private final JsonMapper jsonMapper;
    private final JacksonJsonpMapper jsonpMapper;

    public ElasticsearchWriter(ElasticsearchClientFactory clientFactory, int writeBulkSize, JsonMapper jsonMapper) {
        this.writeBulkSize = writeBulkSize;
        this.jsonMapper = jsonMapper;
        this.jsonpMapper = new JacksonJsonpMapper(jsonMapper);
        this.client = clientFactory.create();
    }

    @Override
    public boolean createIndex(SearchIndex index) {
        try {
            if (existIndex(index.name())) {
                return false;
            }
            CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                    .index(index.name())
                    .settings(fromMap(index.settings(), IndexSettings._DESERIALIZER))
                    .mappings(fromMap(index.mappings(), TypeMapping._DESERIALIZER))
                    .aliases(index.aliases().entrySet().stream()
                            .map(e -> Map.entry(e.getKey(), fromMap((Map<String, Object>) e.getValue(), Alias._DESERIALIZER)))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .build();
            client.indices().create(createIndexRequest);
            return true;
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private boolean existIndex(String name) throws IOException {
        ExistsRequest existsRequest = new ExistsRequest.Builder().index(name).build();
        return client.indices().exists(existsRequest).value();
    }

    public <T> T fromMap(Map<String, Object> map, JsonpDeserializer<T> deserializer) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            jsonMapper.writeValue(outputStream, map);
            return deserializer.deserialize(
                    new JacksonJsonpParser(jsonMapper.createParser(outputStream.toByteArray()), jsonpMapper),
                    jsonpMapper);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SearchDocumentWriter writeDocuments(String index) {
        return new ElasticsearchDocumentWriter(client, index, this.writeBulkSize);
    }

    @Override
    public void close() {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    void refreshIndex(String index) {
        try {
            client.indices().refresh(new RefreshRequest.Builder().index(index).build());
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }
}
