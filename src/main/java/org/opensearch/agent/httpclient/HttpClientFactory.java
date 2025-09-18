/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.httpclient;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.SystemDefaultDnsResolver;
import org.apache.hc.client5.http.impl.DefaultSchemePortResolver;
import org.apache.hc.client5.http.impl.LaxRedirectStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.protocol.HttpContext;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class HttpClientFactory {

    private static final SystemDefaultDnsResolver defaultResolver = SystemDefaultDnsResolver.INSTANCE;

    public static CloseableHttpClient getCloseableHttpClient() {
        return createHttpClient();
    }

    private static CloseableHttpClient createHttpClient() {
        // Create connection manager with custom DNS resolver
        HttpClientConnectionManager connectionManager = PoolingHttpClientConnectionManagerBuilder
            .create()
            .setSchemePortResolver(new DefaultSchemePortResolver() {
                @Override
                public int resolve(HttpHost host) {
                    validateSchemaAndPort(host);
                    return super.resolve(host);
                }
            })
            .setDnsResolver(new DnsResolver() {
                @Override
                public InetAddress[] resolve(String s) throws UnknownHostException {
                    return validateIp(s);
                }

                @Override
                public String resolveCanonicalHostname(String s) throws UnknownHostException {
                    return defaultResolver.resolveCanonicalHostname(s);
                }
            })
            .build();

        // Build HttpClient with the custom connection manager
        return HttpClientBuilder.create().setConnectionManager(connectionManager).setRedirectStrategy(new LaxRedirectStrategy() {
            @Override
            public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) {
                // Do not follow redirects
                return false;
            }
        }).build();
    }

    @VisibleForTesting
    protected static void validateSchemaAndPort(HttpHost host) {
        String scheme = host.getSchemeName();
        if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
            String[] hostNamePort = host.getHostName().split(":");
            if (hostNamePort.length > 1 && NumberUtils.isDigits(hostNamePort[1])) {
                int port = Integer.parseInt(hostNamePort[1]);
                if (port < 0 || port > 65536) {
                    log.error("Remote inference port out of range: " + port);
                    throw new IllegalArgumentException("Port out of range: " + port);
                }
            }
        } else {
            log.error("Remote inference scheme not supported: " + scheme);
            throw new IllegalArgumentException("Unsupported scheme: " + scheme);
        }
    }

    @VisibleForTesting
    protected static InetAddress[] validateIp(String hostName) throws UnknownHostException {
        InetAddress[] addresses = InetAddress.getAllByName(hostName);
        if (hasPrivateIpAddress(addresses)) {
            log.error("Remote inference host name has private ip address: " + hostName);
            throw new IllegalArgumentException(hostName);
        }
        return addresses;
    }

    private static boolean hasPrivateIpAddress(InetAddress[] ipAddress) {
        for (InetAddress ip : ipAddress) {
            if (ip instanceof Inet4Address) {
                byte[] bytes = ip.getAddress();
                if (bytes.length != 4) {
                    return true;
                } else {
                    int firstOctets = bytes[0] & 0xff;
                    int firstInOctal = parseWithOctal(String.valueOf(firstOctets));
                    int firstInHex = Integer.parseInt(String.valueOf(firstOctets), 16);
                    if (firstInOctal == 127 || firstInHex == 127) {
                        return bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 1;
                    } else if (firstInOctal == 10 || firstInHex == 10) {
                        return true;
                    } else if (firstInOctal == 172 || firstInHex == 172) {
                        int secondOctets = bytes[1] & 0xff;
                        int secondInOctal = parseWithOctal(String.valueOf(secondOctets));
                        int secondInHex = Integer.parseInt(String.valueOf(secondOctets), 16);
                        return (secondInOctal >= 16 && secondInOctal <= 32) || (secondInHex >= 16 && secondInHex <= 32);
                    } else if (firstInOctal == 192 || firstInHex == 192) {
                        int secondOctets = bytes[1] & 0xff;
                        int secondInOctal = parseWithOctal(String.valueOf(secondOctets));
                        int secondInHex = Integer.parseInt(String.valueOf(secondOctets), 16);
                        return secondInOctal == 168 || secondInHex == 168;
                    }
                }
            }
        }
        return Arrays.stream(ipAddress).anyMatch(x -> x.isSiteLocalAddress() || x.isLoopbackAddress() || x.isAnyLocalAddress());
    }

    private static int parseWithOctal(String input) {
        try {
            return Integer.parseInt(input, 8);
        } catch (NumberFormatException e) {
            return Integer.parseInt(input);
        }
    }
}
