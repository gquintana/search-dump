package com.github.gquintana.searchdump.s3;

import com.github.gquintana.searchdump.configuration.Configuration;
import com.github.gquintana.searchdump.core.SearchAdapterFactory;


public class S3AdapterFactory implements SearchAdapterFactory {
    @Override
    public S3SearchReader createReader(Configuration configuration) {
        return new S3SearchReader(
                configuration.getString("reader.endpoint-url").orElse(null),
                configuration.getString("reader.region").orElse(null),
                configuration.getString("reader.bucket").orElseThrow(() -> new IllegalArgumentException("Missing reader.bucket")),
                configuration.getString("reader.key").orElseThrow(() -> new IllegalArgumentException("Missing reader.key"))
        );
    }

    @Override
    public S3SearchWriter createWriter(Configuration configuration) {
        return new S3SearchWriter(
                configuration.getString("writer.endpoint-url").orElse(null),
                configuration.getString("writer.region").orElse(null),
                configuration.getString("writer.bucket").orElseThrow(() -> new IllegalArgumentException("Missing writer.bucket")),
                configuration.getString("writer.key").orElseThrow(() -> new IllegalArgumentException("Missing writer.key")),
                configuration.getInt("writer.file-size").orElse(1000)
        );
    }
}
