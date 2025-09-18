/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.httpclient;

import static org.junit.Assert.assertNotNull;

import java.net.UnknownHostException;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.HttpHost;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HttpClientFactoryTests {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test_getCloseableHttpClient_success() {
        CloseableHttpClient client = HttpClientFactory.getCloseableHttpClient();
        assertNotNull(client);
    }

    @Test
    public void test_validateIp_validIp_noException() throws UnknownHostException {
        HttpClientFactory.validateIp("api.openai.com");
    }

    @Test
    public void test_validateIp_invalidIp_throwException() throws UnknownHostException {
        expectedException.expect(UnknownHostException.class);
        HttpClientFactory.validateIp("www.makesureitisaunknownhost.com");
    }

    @Test
    public void test_validateIp_privateIp_throwException() throws UnknownHostException {
        expectedException.expect(IllegalArgumentException.class);
        HttpClientFactory.validateIp("localhost");
    }

    @Test
    public void test_validateIp_rarePrivateIp_throwException() throws UnknownHostException {
        try {
            HttpClientFactory.validateIp("0254.020.00.01");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }

        try {
            HttpClientFactory.validateIp("172.1048577");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }

        try {
            HttpClientFactory.validateIp("2886729729");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }

        try {
            HttpClientFactory.validateIp("192.11010049");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }

        try {
            HttpClientFactory.validateIp("3232300545");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }

        try {
            HttpClientFactory.validateIp("0:0:0:0:0:ffff:127.0.0.1");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void test_validateSchemaAndPort_success() {
        HttpHost httpHost = new HttpHost("https", "api.openai.com", 8080);
        HttpClientFactory.validateSchemaAndPort(httpHost);
    }

    @Test
    public void test_validateSchemaAndPort_notAllowedSchema_throwException() {
        expectedException.expect(IllegalArgumentException.class);
        HttpHost httpHost = new HttpHost("ftp", "api.openai.com", 8080);
        HttpClientFactory.validateSchemaAndPort(httpHost);
    }

    @Test
    public void test_validateSchemaAndPort_portNotInRange_throwException() {
        expectedException.expect(IllegalArgumentException.class);
        HttpHost httpHost = new HttpHost("https", "api.openai.com:65537", -1);
        HttpClientFactory.validateSchemaAndPort(httpHost);
    }
}
