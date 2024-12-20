package com.github.gquintana.searchdump.elasticsearch;

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

import javax.net.ssl.HostnameVerifier;
import java.net.URI;

public class ElasticsearchClientFactory {
    private final String url;
    private final String username;
    private final String password;
    private final boolean sslVerify;

    public ElasticsearchClientFactory(String url, String username, String password, boolean sslVerify) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.sslVerify = sslVerify;
    }

    public ElasticsearchClient create() {
        URI uri = URI.create(url);
        RestClient restClient = RestClient
                .builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()))
                .setHttpClientConfigCallback(this::customizeHttpClient)
                .build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    private HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder clientBuilder) {
        if (username != null && password != null) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
        if (!sslVerify) {
            clientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
        return clientBuilder;
    }
}
