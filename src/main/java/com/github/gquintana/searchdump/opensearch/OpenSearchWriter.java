package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.SearchDocumentWriter;
import com.github.gquintana.searchdump.core.SearchIndex;
import com.github.gquintana.searchdump.core.SearchWriter;
import com.github.gquintana.searchdump.core.TechnicalException;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.json.jackson.JacksonJsonpParser;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class OpenSearchWriter implements SearchWriter {
    private final OpenSearchClient client;
    private final JsonMapper jsonMapper;
    private final JsonpMapper jsonpMapper;
    private final int writeBulkSize;

    public OpenSearchWriter(OpenSearchClientFactory clientFactory,
                            int writeBulkSize, JsonMapper jsonMapper) {
        this.client = clientFactory.create();
        this.jsonMapper = jsonMapper;
        this.jsonpMapper = new JacksonJsonpMapper(jsonMapper);
        this.writeBulkSize = writeBulkSize;
    }


    @Override
    public boolean createIndex(SearchIndex index) {
        try {
            if (existIndex(index.name())) {
                return false;
            }
            CreateIndexRequest.Builder createIndexRequestBuilder = new CreateIndexRequest.Builder()
                    .index(index.name());
            if (index.settings() != null) {
                createIndexRequestBuilder.settings(fromMap(cleanIndexSettings(index.settings()), IndexSettings._DESERIALIZER));
            }
            if (index.mappings() != null) {
                createIndexRequestBuilder.mappings(fromMap(cleanIndexMappings(index.mappings()), TypeMapping._DESERIALIZER));
            }
            if (index.aliases() != null) {
                createIndexRequestBuilder.aliases(index.aliases().entrySet().stream()
                        .map(e -> Map.entry(e.getKey(), fromMap((Map<String, Object>) e.getValue(), Alias._DESERIALIZER)))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            }
            client.indices().create(createIndexRequestBuilder.build());
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> cleanIndexSettings(Map<String, Object> settings) {
        Map<String, Object> cleanedSettings = new HashMap<>(settings);
        cleanedSettings.remove("lifecycle"); // ILM is not supported on Opensearch
        if (cleanedSettings.containsKey("index")) {
            cleanedSettings.put("index", cleanIndexSettings((Map<String, Object>) cleanedSettings.get("index")));
        }
        if (cleanedSettings.containsKey("settings")) {
            cleanedSettings.put("settings", cleanIndexSettings((Map<String, Object>) cleanedSettings.get("settings")));
        }
        return cleanedSettings;
    }

    private Map<String, Object> cleanIndexMappings(Map<String, Object> mappings) {
        Map<String, Object> cleanedMappings = new HashMap<>(mappings);
        if (cleanedMappings.containsKey("dynamic")) {
            cleanedMappings.put("dynamic", String.valueOf(cleanedMappings.get("dynamic")));
        }
        return cleanedMappings;
    }

    private boolean existIndex(String name) throws IOException {
        ExistsRequest existsRequest = new ExistsRequest.Builder().index(name).build();
        return client.indices().exists(existsRequest).value();
    }

    private <T> T fromMap(Map<String, Object> map, JsonpDeserializer<T> deserializer) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            jsonMapper.writeValue(outputStream, map);
            return deserializer.deserialize(
                    new JacksonJsonpParser(jsonMapper.createParser(outputStream.toByteArray())),
                    jsonpMapper);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SearchDocumentWriter writeDocuments(String index) {
        return new OpenSearchDocumentWriter(client, index, this.writeBulkSize);
    }

    void refreshIndex(String index) {
        try {
            client.indices().refresh(new RefreshRequest.Builder().index(index).build());
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public void close() {
        try {
            client._transport().close();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }
}
