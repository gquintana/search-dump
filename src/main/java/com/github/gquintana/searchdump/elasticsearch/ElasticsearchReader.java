package com.github.gquintana.searchdump.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.json.JsonpSerializable;
import co.elastic.clients.json.jackson.JacksonJsonpGenerator;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;
import org.example.core.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchReader implements SearchReader, QuietCloseable {
    private final int searchPageSize;
    private final String searchScrollTime;
    private final ElasticsearchClient client;
    private final JsonMapper jsonMapper;
    private final JacksonJsonpMapper jsonpMapper;

    public ElasticsearchReader(String url, String username, String password, boolean sslVerify,
                               int searchPageSize, String searchScrollTime, JsonMapper jsonMapper) {
        this.searchPageSize = searchPageSize;
        this.searchScrollTime = searchScrollTime;
        this.jsonMapper = jsonMapper;
        this.client = new ElasticsearchClientFactory(url, username, password, sslVerify).create();
        this.jsonpMapper = new JacksonJsonpMapper(jsonMapper);
    }

    @Override
    public SearchIndex getIndex(String name) {
        try {
            GetIndexRequest getIndexRequest = new GetIndexRequest.Builder().index(name).build();
            GetIndexResponse getIndexResponse = client.indices().get(getIndexRequest);
            Map<String, Object>  settings = cleanSettings(toMap(getIndexResponse.get(name).settings()));
            Map<String, Object>  aliases = getIndexResponse.get(name).aliases().entrySet()
                    .stream().map(e -> Map.entry(e.getKey(), toMap(e.getValue())))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Map<String, Object>  mappings = toMap(getIndexResponse.get(name).mappings());
            return new SearchIndex(name, settings, mappings, aliases);
        } catch (IOException e) {
            throw new TechnicalException(e);
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
        return new ElasticsearchDocumentReader(index, searchPageSize, searchScrollTime, client);
    }

    @Override
    public void close()  {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }
}