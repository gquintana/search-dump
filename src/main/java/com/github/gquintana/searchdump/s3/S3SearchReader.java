package com.github.gquintana.searchdump.s3;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class S3SearchReader implements SearchReader<S3SearchDocumentPartition>, QuietCloseable {
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
    public List<String> listIndices(List<String> indices) {
        final String prefix = key + "/";
        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket).prefix(prefix).build());
        MultiGlobMatcher multiGlobMatcher = new MultiGlobMatcher(indices);
        return listObjectsResponse.contents().stream()
                .flatMap(o -> {
                    String n = o.key().substring(prefix.length());
                    int slashIndex = n.lastIndexOf('/');
                    if (slashIndex == 0) {
                        throw new IllegalStateException("Invalid key: " + n);
                    } else if (slashIndex > 0) {
                        n = n.substring(0, slashIndex);
                    }
                    return multiGlobMatcher.matches(n) ? Stream.of(n) : Stream.empty();
                })
                .distinct()
                .toList();
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

    private List<S3Object> listDocumentsS3Objects(String index) {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(String.format("%s/%s/documents-", key, index))
                .build();
        ListObjectsV2Response listObjectsResponse = this.s3Client.listObjectsV2(listObjectsRequest);
        return listObjectsResponse.contents().stream()
                .filter(o -> o.key().endsWith(".json.gz"))
                .sorted(Comparator.comparing(S3Object::key))
                .toList();
    }

    @Override
    public SearchDocumentReader readDocuments(String index) {
        return new S3SearchDocumentReader(jsonMapper, s3Client, bucket, listDocumentsS3Objects(index));
    }

    @Override
    public void close() {
        s3Client.close();
    }

    @Override
    public List<S3SearchDocumentPartition> splitDocuments(String index, int partitionCount) {
        final AtomicInteger partitionIndex = new AtomicInteger();
        return ListSplitter.split(listDocumentsS3Objects(index), partitionCount)
                .stream()
                .map(o -> new S3SearchDocumentPartition(index, partitionIndex.getAndIncrement(), partitionCount, o))
                .toList();
    }

    @Override
    public SearchDocumentReader readDocuments(S3SearchDocumentPartition partition) {
        return new S3SearchDocumentReader(jsonMapper, s3Client, bucket, partition.documentsS3Objects());
    }
}
