package com.composum.platform.replication.remote;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Arrays;

public class CheckHttpClientAuthBehavior {

    public static void main(String[] args) throws Exception {
        new CheckHttpClientAuthBehavior().run();
    }

    private void run() throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            URI uri = new URI("http://localhost:10010/bin/cpm/platform/replication/publishreceiver.releaseInfo.json?releaseRoot=%2Fcontent%2Fist%2Fcomposum&sourcePath=%2Fcontent%2Fist%2Fcomposum&targetPath=%2Fpublic%2Fist%2Fcomposum");
            HttpGet request = new HttpGet(uri);

            HttpClientContext context = createHttpClientContext(uri);

            try (CloseableHttpResponse response = client.execute(request, context)) {
                System.out.println(Arrays.asList(response.getAllHeaders()).toString());
                response.getEntity().writeTo(System.out);
                System.out.println();
            }

            //OK: this does not lead to another login
            try (CloseableHttpResponse response = client.execute(request, context)) {
                System.out.println(Arrays.asList(response.getAllHeaders()).toString());
                response.getEntity().writeTo(System.out);
                System.out.println();
            }
            try (CloseableHttpResponse response = client.execute(request, context)) {
                System.out.println(Arrays.asList(response.getAllHeaders()).toString());
                response.getEntity().writeTo(System.out);
                System.out.println();
            }

            // Bad: this leads to another login.
            try (CloseableHttpResponse response = client.execute(request, createHttpClientContext(uri))) {
                System.out.println(Arrays.asList(response.getAllHeaders()).toString());
                response.getEntity().writeTo(System.out);
                System.out.println();
            }
            // so we have to keep the context around.
        }
        System.out.flush();
    }

    @Nonnull
    private HttpClientContext createHttpClientContext(URI uri) {
        AuthScope authScope = new AuthScope(uri.getHost(), uri.getPort());
        HttpClientContext context = HttpClientContext.create();
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(authScope, new UsernamePasswordCredentials("admin", "admin"));
        context.setCredentialsProvider(credsProvider);
        return context;
    }
}
