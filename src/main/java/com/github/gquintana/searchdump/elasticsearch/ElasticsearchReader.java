package com.github.gquintana.searchdump.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cat.IndicesRequest;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.json.JsonpSerializable;
import co.elastic.clients.json.jackson.JacksonJsonpGenerator;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticsearchReader implements SearchReader, QuietCloseable {
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {};
    private final int searchPageSize;
    private final String searchScrollTime;
    private final ElasticsearchClient client;
    private final JsonMapper jsonMapper;
    private final JacksonJsonpMapper jsonpMapper;

    public ElasticsearchReader(ElasticsearchClientFactory clientFactory,
                               int searchPageSize, String searchScrollTime, JsonMapper jsonMapper) {
        this.searchPageSize = searchPageSize;
        this.searchScrollTime = searchScrollTime;
        this.jsonMapper = jsonMapper;
        this.client = clientFactory.create();
        this.jsonpMapper = new JacksonJsonpMapper(jsonMapper);
    }

    public List<String> listIndices(List<String> names) {
        try {
            IndicesResponse indicesResponse = client.cat().indices(new IndicesRequest.Builder().index(names).build());
            return indicesResponse.valueBody().stream().map(IndicesRecord::index).toList();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
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
        if (jsonpSerializable == null) {
            return null;
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            JacksonJsonpGenerator jsonpGenerator = new JacksonJsonpGenerator(jsonMapper.createGenerator(outputStream));
            jsonpSerializable.serialize(
                    jsonpGenerator,
                    jsonpMapper);
            jsonpGenerator.flush();
            return jsonMapper.readValue(outputStream.toByteArray(), MAP_TYPE_REF);
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
