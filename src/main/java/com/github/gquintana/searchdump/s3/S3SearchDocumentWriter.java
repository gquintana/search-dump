package com.github.gquintana.searchdump.s3;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentWriter;
import com.github.gquintana.searchdump.core.TechnicalException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

public class S3SearchDocumentWriter implements SearchDocumentWriter {
    private final JsonMapper jsonMapper;
    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private final String index;
    private final int fileSize;
    private int fileCount;
    private int fileLines;
    private OutputStream tempOutputStream;
    private Path tempFile;

    public S3SearchDocumentWriter(JsonMapper jsonMapper, S3Client s3Client, String bucket, String key, String index, int fileSize) {
        this.jsonMapper = jsonMapper;
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
        this.index = index;
        this.fileSize = fileSize;

    }

    private void startS3Object() throws IOException {
        tempFile = Files.createTempFile(index + "-documents-", ".json.gz");
        this.tempOutputStream = new GZIPOutputStream(Files.newOutputStream(tempFile));
        this.fileLines = 0;
    }

    @Override
    public void write(SearchDocument document) {
        try {
            if (tempFile == null) {
                startS3Object();
            }
            jsonMapper.writeValue(tempOutputStream, document);
            tempOutputStream.write('\n');
            fileLines++;
            if (fileLines >= fileSize) {
                endS3Object();
            }
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public void flush() {
        try {
            endS3Object();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private void endS3Object() throws IOException {
        tempOutputStream.close();
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(String.format("%s/%s/documents-%06d.json.gz", key, index, fileCount++))
                .build();
        s3Client.putObject(putObjectRequest, tempFile);
        Files.deleteIfExists(tempFile);
        tempFile = null;
        tempOutputStream = null;
    }

    @Override
    public void close() {
        if (tempOutputStream != null) {
            try {
                endS3Object();
            } catch (IOException e) {
                throw new TechnicalException(e);
            }
        }
    }
}
