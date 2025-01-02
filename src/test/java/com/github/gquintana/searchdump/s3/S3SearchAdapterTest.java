package com.github.gquintana.searchdump.s3;

import com.github.gquintana.searchdump.core.AbstractAdapterTest;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@Testcontainers
class S3SearchAdapterTest extends AbstractAdapterTest<S3SearchWriter, S3SearchReader> {
    static final Logger LOGGER = LoggerFactory.getLogger(S3SearchAdapterTest.class);
    @Container
    static final LocalStackContainer container = new LocalStackContainer("0.11.3")
            .withServices(LocalStackContainer.Service.S3);
    static final String TEST_BUCKET = "test-bucket";
    public static final String TEST_PATH = "test-path";

    @BeforeAll
    static void initBucket() {
        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), container.getAccessKey());
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), container.getSecretKey());
        S3ClientFactory clientFactory = createClientFactory();
        try (S3Client s3Client = clientFactory.create()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
        }
    }

    @Override
    protected S3SearchReader createReader() {
        return new S3SearchReader(createClientFactory(), TEST_BUCKET, TEST_PATH);
    }

    @Override
    protected S3SearchWriter createWriter() {
        return new S3SearchWriter(createClientFactory(), TEST_BUCKET, TEST_PATH, 10);
    }

    private static S3ClientFactory createClientFactory() {
        return new S3ClientFactory(container.getEndpoint().toString(), container.getRegion());
    }

    @Override
    protected void deleteIndex(String index) {
        S3ClientFactory clientFactory = createClientFactory();
        try (S3Client s3Client = clientFactory.create()) {
            ObjectIdentifier[] ids = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                            .bucket(TEST_BUCKET)
                            .prefix(TEST_PATH + "/" + index + "/")
                            .build())
                    .contents().stream()
                    .map(S3Object::key)
                    .sorted()
                    .map(k -> ObjectIdentifier.builder().key(k).build())
                    .toArray(ObjectIdentifier[]::new);
            s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(TEST_BUCKET).delete(Delete.builder().objects(ids).build()).build());
        }
    }
}