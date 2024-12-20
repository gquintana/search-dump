package com.github.gquintana.searchdump.s3;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentReader;
import com.github.gquintana.searchdump.core.TechnicalException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class S3SearchDocumentReader implements SearchDocumentReader {
    private final JsonMapper jsonMapper;
    private final S3Client s3Client;
    private final String bucket;
    private Iterator<S3Object> s3ObjectIterator;
    private BufferedReader bufferedReader;
    private SearchDocument nextDocument;

    public S3SearchDocumentReader(JsonMapper jsonMapper, S3Client s3Client, String bucket, String key, String index) {
        this.jsonMapper = jsonMapper;
        this.s3Client = s3Client;
        this.bucket = bucket;
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(String.format("%s/%s/documents-", key, index))
                .build();
        ListObjectsV2Response listObjectsResponse = this.s3Client.listObjectsV2(listObjectsRequest);
        s3ObjectIterator = listObjectsResponse.contents().stream()
                .filter(o -> o.key().endsWith(".json.gz"))
                .sorted(Comparator.comparing(S3Object::key))
                .iterator();
        try {
            bufferedReader = nextS3Object();
            nextDocument = nextDocument();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private SearchDocument nextDocument() throws IOException {
        if (bufferedReader == null) {
            return null;
        }
        String line = bufferedReader.readLine();
        if (line == null) {
            return null;
        }
        return jsonMapper.readValue(line, SearchDocument.class);
    }

    private BufferedReader nextS3Object() throws IOException {
        if (!s3ObjectIterator.hasNext()) {
            return null;
        }
        S3Object s3Object = s3ObjectIterator.next();
        ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(GetObjectRequest.builder()
                .bucket(this.bucket)
                .key(s3Object.key())
                .build());
        return new BufferedReader(new InputStreamReader(new GZIPInputStream(inputStream)));
    }

    @Override
    public void close() {
        try {
            closeS3Object();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextDocument != null;
    }

    @Override
    public SearchDocument next() {
        try {
            SearchDocument result = nextDocument;
            nextDocument = nextDocument();
            if (nextDocument == null) {
                closeS3Object();
                bufferedReader = nextS3Object();
                nextDocument = nextDocument();
            }
            return result;
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private void closeS3Object() throws IOException {
        if (bufferedReader != null) {
            bufferedReader.close();
            bufferedReader = null;
        }
    }
}
