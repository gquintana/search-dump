package com.github.gquintana.searchdump.s3;

import com.github.gquintana.searchdump.SearchPortHelper;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
class S3SearchAdapterTest {
    @Container
    final LocalStackContainer container = new LocalStackContainer("0.11.3")
            .withServices(LocalStackContainer.Service.S3);

    @Test
    void testExportImport() throws Exception {
        var helper = new SearchPortHelper("test-1");
        System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), container.getAccessKey());
        System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), container.getSecretKey());
        S3ClientFactory clientFactory = createClientFactory();
        try(S3Client s3Client = clientFactory.create()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket("test-bucket").build());
        }
        try(S3SearchWriter writer = new S3SearchWriter(clientFactory, "test-bucket", "test-path", 10)) {
            helper.createAndFill(writer);
        }
        try(S3SearchReader reader = new S3SearchReader(clientFactory, "test-bucket", "test-path")) {
            helper.readAndCheck(reader);
        }
    }

    private S3ClientFactory createClientFactory() {
        return new S3ClientFactory(container.getEndpoint().toString(), container.getRegion());
    }
}