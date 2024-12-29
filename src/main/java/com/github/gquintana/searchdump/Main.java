package com.github.gquintana.searchdump;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.configuration.*;
import com.github.gquintana.searchdump.core.SearchAdapterFactory;
import com.github.gquintana.searchdump.core.SearchCopier;
import com.github.gquintana.searchdump.core.SearchReader;
import com.github.gquintana.searchdump.core.SearchWriter;
import com.github.gquintana.searchdump.elasticsearch.ElasticsearchFactory;
import com.github.gquintana.searchdump.opensearch.OpenSearchFactory;
import com.github.gquintana.searchdump.s3.S3AdapterFactory;
import com.github.gquintana.searchdump.zipfile.ZipFileAdapterFactory;

import java.nio.file.Path;
import java.util.List;

public class Main {
    public static void main(String ... args) {
        Configuration configuration = createConfiguration(args);
        JsonMapper jsonMapper = JsonMapper.builder().build();
        try(SearchReader reader = createReader(configuration, jsonMapper);
            SearchWriter writer = createWriter(configuration, jsonMapper)) {
            SearchCopier copier = new SearchCopier(reader, writer);
            List<String> indices = configuration.getStrings("index");
            if (indices.isEmpty()) {
                throw new MissingConfigurationException("index");
            }
            copier.copy(indices);
        }
    }

    private static Configuration createConfiguration(String[] args) {
        Configuration configuration = switch (args.length) {
            case 0 -> new EnvironmentConfiguration();
            case 1 -> PropertiesConfiguration.load(Path.of(args[0]));
            default -> new ArgsConfiguration(args);
        };
        return new CompositeConfiguration(configuration, new EnvironmentConfiguration());
    }

    private static SearchWriter createWriter(Configuration configuration, JsonMapper jsonMapper) {
        String type = getType(configuration, "writer");
        return createFactory(type, jsonMapper).createWriter(configuration);
    }

    private static SearchReader createReader(Configuration configuration, JsonMapper jsonMapper) {
        String type = getType(configuration, "reader");
        return createFactory(type, jsonMapper).createReader(configuration);
    }

    private static SearchAdapterFactory createFactory(String type, JsonMapper jsonMapper) {
        return switch (type) {
            case "zip" -> new ZipFileAdapterFactory();
            case "elasticsearch" -> new ElasticsearchFactory(jsonMapper);
            case "opensearch" -> new OpenSearchFactory(jsonMapper);
            case "s3" -> new S3AdapterFactory();
            default -> throw new IllegalArgumentException("Unknown type " + type);
        };
    }

    private static String getType(Configuration configuration, String prefix) {
        final String typeKey = prefix + ".type";
        return configuration.getString(typeKey)
                .orElseThrow(() -> new MissingConfigurationException(typeKey));
    }
}