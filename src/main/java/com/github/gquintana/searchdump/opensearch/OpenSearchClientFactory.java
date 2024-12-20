package com.github.gquintana.searchdump.opensearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import com.github.gquintana.searchdump.core.TechnicalException;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.rest_client.RestClientTransport;

import java.net.URI;
import java.security.GeneralSecurityException;

public class OpenSearchClientFactory {
    private final String url;
    private final String username;
    private final String password;
    private final boolean sslVerify;

    public OpenSearchClientFactory(String url, String username, String password, boolean sslVerify) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.sslVerify = sslVerify;
    }

    public OpenSearchClient create() {
        URI uri = URI.create(this.url);
        RestClient restClient = RestClient
                .builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()))
                .setHttpClientConfigCallback(this::customizeHttpClient)
                .build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new OpenSearchClient(transport);
    }

    private HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder clientBuilder) {
        if (username != null && password != null) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
        if (!sslVerify) {
            try {
                SSLContextBuilder sslContextBuilder = SSLContextBuilder.create();
                sslContextBuilder.loadTrustMaterial(new TrustAllStrategy());
                clientBuilder.setSSLContext(sslContextBuilder.build());
            } catch (GeneralSecurityException e) {
                throw new TechnicalException(e);
            }
            clientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }
        return clientBuilder;
    }
}
