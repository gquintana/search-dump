package com.github.gquintana.searchdump.elasticsearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.configuration.Configuration;
import com.github.gquintana.searchdump.configuration.MissingConfigurationException;
import com.github.gquintana.searchdump.core.SearchFactory;

public class ElasticsearchFactory implements SearchFactory {
    private final JsonMapper jsonMapper;

    public ElasticsearchFactory(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    private ElasticsearchClientFactory createClientFactory(Configuration configuration, String prefix) {
        return new ElasticsearchClientFactory(
                configuration.getString(prefix + ".url").orElseThrow(() -> new MissingConfigurationException(prefix + ".url")),
                configuration.getString(prefix + ".username").orElse(null),
                configuration.getString(prefix + ".password").orElse(null),
                configuration.getBoolean(prefix + ".ssl-verify").orElse(true)
        );
    }

    @Override
    public ElasticsearchReader createReader(Configuration configuration) {
        return new ElasticsearchReader(
                createClientFactory(configuration, "reader"),
                configuration.getInt("reader.page.size").orElse(1000),
                configuration.getString("reader.scroll.time").orElse("5m"),
                jsonMapper
        );
    }

    @Override
    public ElasticsearchWriter createWriter(Configuration configuration) {
        return new ElasticsearchWriter(
                createClientFactory(configuration, "writer"),
                configuration.getInt("writer.bulk.size").orElse(1000),
                jsonMapper
        );
    }
}
