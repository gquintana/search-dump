package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.SearchDocumentReader;
import com.github.gquintana.searchdump.core.SearchIndex;
import com.github.gquintana.searchdump.core.SearchReader;
import com.github.gquintana.searchdump.core.TechnicalException;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.GetIndexRequest;
import org.opensearch.client.opensearch.indices.GetIndexResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class OpenSearchReader implements SearchReader {
    private final OpenSearchClient client;
    private final JsonMapper jsonMapper;
    private final JsonpMapper jsonpMapper;
    private final int searchSize;
    private final String searchScrollTime;

    public OpenSearchReader(OpenSearchClientFactory clientFactory,
                            int searchSize, String searchScrollTime, JsonMapper jsonMapper) {
        this.client = clientFactory.create();
        this.jsonMapper = jsonMapper;
        this.jsonpMapper = new JacksonJsonpMapper(jsonMapper);
        this.searchSize = searchSize;
        this.searchScrollTime = searchScrollTime;
    }

    @Override
    public SearchIndex getIndex(String name) {
        try {
            GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index(name).build();
            GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest);
            Map<String, Object> settings = cleanSettings(toMap(getIndexResponse.get(name).settings()));
            Map<String, Object> aliases = getIndexResponse.get(name).aliases().entrySet()
                    .stream().map(e -> Map.entry(e.getKey(), toMap(e.getValue())))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Map<String, Object> mappings = toMap(getIndexResponse.get(name).mappings());
            return new SearchIndex(name, settings, mappings, aliases);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static Map<String, Object> cleanSettings(Map<String, Object> settings) {
        Map<String, Object> indexSettings = new HashMap<>((Map<String, Object>) settings.get("index"));
        indexSettings.remove("creation_date");
        indexSettings.remove("uuid");
        indexSettings.remove("provided_name");
        indexSettings.remove("version");
        Map<String, Object> result = new HashMap<>(settings);
        result.put("index", indexSettings);
        return result;
    }

    private Map<String, Object> toMap(JsonpSerializable jsonpSerializable) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            JacksonJsonpGenerator jsonpGenerator = new JacksonJsonpGenerator(jsonMapper.createGenerator(outputStream));
            jsonpSerializable.serialize(
                    jsonpGenerator,
                    jsonpMapper);
            jsonpGenerator.flush();
            return jsonMapper.readValue(outputStream.toByteArray(), Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SearchDocumentReader readDocuments(String index) {
        return new OpenSearchDocumentReader(index, searchSize, searchScrollTime, client);
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