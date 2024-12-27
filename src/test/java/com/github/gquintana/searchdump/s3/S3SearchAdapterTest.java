package com.github.gquintana.searchdump.s3;

import com.github.gquintana.searchdump.AbstractAdapterTest;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
class S3SearchAdapterTest extends AbstractAdapterTest<S3SearchWriter, S3SearchReader> {
    static final Logger LOGGER = LoggerFactory.getLogger(S3SearchAdapterTest.class);
    @Container
    static final LocalStackContainer container = new LocalStackContainer("0.11.3")
            .withServices(LocalStackContainer.Service.S3);

    @BeforeAll
    static void initBucket() {
        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), container.getAccessKey());
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), container.getSecretKey());
        S3ClientFactory clientFactory = createClientFactory();
        try(S3Client s3Client = clientFactory.create()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket("test-bucket").build());
        } catch (Exception e) {
            LOGGER.warn(e.getMessage());
        }
    }

    @Override
    protected S3SearchReader createReader() {
        return new S3SearchReader(createClientFactory(), "test-bucket", "test-path");
    }

    @Override
    protected S3SearchWriter createWriter() {
        return new S3SearchWriter(createClientFactory(), "test-bucket", "test-path", 10);
    }

    private static S3ClientFactory createClientFactory() {
        return new S3ClientFactory(container.getEndpoint().toString(), container.getRegion());
    }
}