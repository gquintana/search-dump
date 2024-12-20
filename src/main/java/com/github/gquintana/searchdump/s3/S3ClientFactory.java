package com.github.gquintana.searchdump.s3;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

public class S3ClientFactory {
    private final String endpointUrl;
    private final String region;

    public S3ClientFactory(String endpointUrl, String region) {
        this.endpointUrl = endpointUrl;
        this.region = region;
    }

    public S3Client create() {
        S3ClientBuilder builder = S3Client.builder();
        if (endpointUrl != null) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        if (region != null) {
            builder.region(Region.of(region));
        }
        return builder.build();
    }
}
