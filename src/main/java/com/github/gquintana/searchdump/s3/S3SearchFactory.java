package com.github.gquintana.searchdump.s3;

import com.github.gquintana.searchdump.configuration.Configuration;
import com.github.gquintana.searchdump.configuration.MissingConfigurationException;
import com.github.gquintana.searchdump.core.SearchFactory;


public class S3SearchFactory implements SearchFactory {
    private S3ClientFactory createClientFactory(Configuration configuration, String prefix) {
        return new S3ClientFactory(
                configuration.getString(prefix+".endpoint.url").orElse(null),
                configuration.getString(prefix+".region").orElse(null));
    }
    @Override
    public S3SearchReader createReader(Configuration configuration) {
        return new S3SearchReader(
                createClientFactory(configuration, "reader"),
                configuration.getString("reader.bucket").orElseThrow(() -> new MissingConfigurationException("reader.bucket")),
                configuration.getString("reader.key").orElseThrow(() -> new MissingConfigurationException("reader.key"))
        );
    }

    @Override
    public S3SearchWriter createWriter(Configuration configuration) {
        return new S3SearchWriter(
                createClientFactory(configuration, "writer"),
                configuration.getString("writer.bucket").orElseThrow(() -> new MissingConfigurationException("writer.bucket")),
                configuration.getString("writer.key").orElseThrow(() -> new MissingConfigurationException("writer.key")),
                configuration.getInt("writer.file.size").orElse(1000)
        );
    }
}
