package com.github.gquintana.searchdump.s3;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;

public class S3SearchWriter implements SearchWriter, QuietCloseable {
    private final JsonMapper jsonMapper;
    private final int writeFileSize;
    private final S3Client s3Client;
    private final String bucket;
    private final String key;

    public S3SearchWriter(S3ClientFactory clientFactory, String bucket, String key, int writeFileSize) {
        this.s3Client = clientFactory.create();
        this.bucket = bucket;
        this.key = key;
        this.writeFileSize = writeFileSize;
        jsonMapper = JsonMapper.builder().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false).build();
    }


    @Override
    public void createIndex(SearchIndex index) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key + "/" + index.name() + "/index.json")
                    .build();
            RequestBody requestBody = RequestBody.fromBytes(jsonMapper.writeValueAsBytes(index));
            s3Client.putObject(putObjectRequest, requestBody);
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public SearchDocumentWriter writeDocuments(String index) {
        return new S3SearchDocumentWriter(jsonMapper, s3Client, bucket, key, index, writeFileSize);
    }

    @Override
    public void close() {
        s3Client.close();
    }
}
