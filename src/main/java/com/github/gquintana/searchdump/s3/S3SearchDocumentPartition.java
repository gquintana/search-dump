package com.github.gquintana.searchdump.s3;

import com.github.gquintana.searchdump.core.SearchDocumentPartition;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

public class S3SearchDocumentPartition extends SearchDocumentPartition {
    private final List<S3Object> documentsS3Objects;

    public S3SearchDocumentPartition(String index, int partitionIndex, int partitionCount, List<S3Object> documentsS3Objects) {
        super(index, partitionIndex, partitionCount);
        this.documentsS3Objects = documentsS3Objects;
    }

    public List<S3Object> documentsS3Objects() {
        return documentsS3Objects;
    }
}
