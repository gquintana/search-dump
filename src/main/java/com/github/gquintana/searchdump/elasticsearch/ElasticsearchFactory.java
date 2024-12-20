package com.github.gquintana.searchdump.elasticsearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.configuration.Configuration;
import com.github.gquintana.searchdump.core.SearchAdapterFactory;

public class ElasticsearchFactory implements SearchAdapterFactory {
    private final JsonMapper jsonMapper;

    public ElasticsearchFactory(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    public ElasticsearchReader createReader(Configuration configuration) {
        return new ElasticsearchReader(
                configuration.getString("reader.url").orElseThrow(() -> new IllegalArgumentException("Missing reader.url")),
                configuration.getString("reader.username").orElse(null),
                configuration.getString("reader.password").orElse(null),
                configuration.getBoolean("reader.ssl-verify").orElse(true),
                configuration.getInt("reader.page-size").orElse(1000),
                configuration.getString("reader.scroll-time").orElse("5m"),
                jsonMapper
        );
    }

    @Override
    public ElasticsearchWriter createWriter(Configuration configuration) {
        return new ElasticsearchWriter(
                configuration.getString("writer.url").orElseThrow(() -> new IllegalArgumentException("Missing reader.url")),
                configuration.getString("writer.username").orElse(null),
                configuration.getString("writer.password").orElse(null),
                configuration.getBoolean("writer.ssl-verify").orElse(true),
                configuration.getInt("writer.bulk-size").orElse(1000),
                jsonMapper
        );
    }
}
