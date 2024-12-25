package com.github.gquintana.searchdump.s3;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class S3SearchReader implements SearchReader, QuietCloseable {
    private final JsonMapper jsonMapper;
    private final S3Client s3Client;
    private final String bucket;
    private final String key;


    public S3SearchReader(S3ClientFactory clientFactory, String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
        jsonMapper = JsonMapper.builder().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false).build();
        s3Client = clientFactory.create();

    }

    @Override
    public SearchIndex getIndex(String name) {
        try (InputStream inputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key + "/" + name + "/index.json").build())) {
            Map<String, Object> index = jsonMapper.readValue(inputStream, Map.class);
            return new SearchIndex((String) index.get("name"),
                    (Map<String, Object>) index.get("settings"),
                    (Map<String, Object>) index.get("mappings"),
                    (Map<String, Object>) index.get("aliases"));
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public SearchDocumentReader readDocuments(String index) {
        return new S3SearchDocumentReader(jsonMapper, s3Client, bucket, key, index);
    }

    @Override
    public void close() {
        s3Client.close();
    }
}
