package com.github.gquintana.searchdump.s3;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
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
    public boolean createIndex(SearchIndex index) {
        String indexKey = key + "/" + index.name() + "/index.json";
        try {
            if (existObject(indexKey)) {
                return false;
            }
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(indexKey)
                    .build();
            RequestBody requestBody = RequestBody.fromBytes(jsonMapper.writeValueAsBytes(index));
            s3Client.putObject(putObjectRequest, requestBody);
            return true;
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private boolean existObject(String key) {
        try {
            HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build());
            return !headObjectResponse.deleteMarker();
        } catch (NoSuchKeyException e) {
            return false;
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
