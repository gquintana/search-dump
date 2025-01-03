package com.github.gquintana.searchdump;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.configuration.*;
import com.github.gquintana.searchdump.core.SearchCopier;
import com.github.gquintana.searchdump.core.SearchFactory;
import com.github.gquintana.searchdump.core.SearchReader;
import com.github.gquintana.searchdump.core.SearchWriter;
import com.github.gquintana.searchdump.elasticsearch.ElasticsearchFactory;
import com.github.gquintana.searchdump.opensearch.OpenSearchFactory;
import com.github.gquintana.searchdump.s3.S3SearchFactory;
import com.github.gquintana.searchdump.zipfile.ZipFileSearchFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String... args) {
        Configuration configuration = createConfiguration(args);
        JsonMapper jsonMapper = JsonMapper.builder().build();
        try (SearchReader reader = createReader(configuration, jsonMapper);
             SearchWriter writer = createWriter(configuration, jsonMapper)) {
            SearchCopier copier = new SearchCopier(reader, writer,
                    configuration.getBoolean("index.skip.failed").orElse(Boolean.TRUE),
                    configuration.getBoolean("index.skip.existing").orElse(Boolean.TRUE));
            List<String> indices = configuration.getStrings("index.names");
            if (indices.isEmpty()) {
                throw new MissingConfigurationException("index.names");
            }
            copier.copy(indices);
        }
    }

    private static Configuration createConfiguration(String[] args) {
        List<Configuration> configurations = new ArrayList<>();
        configurations.add(new EnvironmentConfiguration());
        if (args.length == 1) {
            configurations.add(PropertiesConfiguration.load(Path.of(args[0])));
        } else if (args.length > 1) {
            ArgsConfiguration argsConfiguration = new ArgsConfiguration(args);
            argsConfiguration.getString("config")
                .ifPresent(s -> configurations.add(PropertiesConfiguration.load(Path.of(s))));
            configurations.add(argsConfiguration);
        }
        Collections.reverse(configurations);
        return new CompositeConfiguration(configurations);
    }

    private static SearchWriter createWriter(Configuration configuration, JsonMapper jsonMapper) {
        String type = getType(configuration, "writer");
        return createFactory(type, jsonMapper).createWriter(configuration);
    }

    private static SearchReader createReader(Configuration configuration, JsonMapper jsonMapper) {
        String type = getType(configuration, "reader");
        return createFactory(type, jsonMapper).createReader(configuration);
    }

    private static SearchFactory createFactory(String type, JsonMapper jsonMapper) {
        return switch (type) {
            case "zip" -> new ZipFileSearchFactory();
            case "elasticsearch" -> new ElasticsearchFactory(jsonMapper);
            case "opensearch" -> new OpenSearchFactory(jsonMapper);
            case "s3" -> new S3SearchFactory();
            default -> throw new IllegalArgumentException("Unknown type " + type);
        };
    }

    private static String getType(Configuration configuration, String prefix) {
        final String typeKey = prefix + ".type";
        return configuration.getString(typeKey)
                .orElseThrow(() -> new MissingConfigurationException(typeKey));
    }
}