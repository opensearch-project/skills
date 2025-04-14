/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.opensearch.agent.ToolPlugin;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(WebSearchTool.TYPE)
public class WebSearchTool implements Tool {

    public static final String TYPE = "WebSearchTool";

    private static final String DEFAULT_DESCRIPTION = "A generic web search tool that supports multiple search engines and endpoints. "
        + "Parameters: {"
        + "\\\"query\\\": \\\"search terms\\\", "
        + "\\\"engine\\\": \\\"google|duckduckgo|bing\\\", "
        + "\\\"next_page\\\": \\\"search result next page link\\\""
        + "}";

    private static final String USER_AGENT = "OpenSearchWebCrawler/1.0";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;
    private CloseableHttpClient httpClient;

    private final ThreadPool threadPool;
    private Map<String, Object> attributes;

    public WebSearchTool(ThreadPool threadPool) {
        this.httpClient = HttpClients.createDefault();
        this.threadPool = threadPool;
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        // common search parameters
        String query = parameters.getOrDefault("query", parameters.get("question")).replaceAll(" ", "+");
        String engine = parameters.getOrDefault("engine", "google");
        String endpoint = parameters.getOrDefault("endpoint", getDefaultEndpoint(engine));
        String apiKey = parameters.getOrDefault("api_key", null);
        String nextPage = parameters.getOrDefault("next_page", null);

        // Token information of the target link to crawl
        String token = parameters.getOrDefault("token", null);

        // Google search parameters
        String engineId = parameters.getOrDefault("engine_id", null);
        threadPool.executor(ToolPlugin.WEBSEARCH_CRAWLER_THREADPOOL).submit(() -> {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    String parsedNextPage = null;
                    if ("duckduckgo".equalsIgnoreCase(engine)) {
                        // duckduckgo has different approach to other APIs as it's not a standard public API.
                        if (nextPage != null) {
                            fetchDuckDuckGoResult(nextPage, token, listener);
                        } else {
                            fetchDuckDuckGoResult(buildDDGEndpoint(getDefaultEndpoint(engine), query), token, listener);
                        }
                    } else {
                        HttpGet getRequest = null;
                        if ("google".equalsIgnoreCase(engine)) {
                            if (nextPage != null) {
                                getRequest = new HttpGet(nextPage);
                                parsedNextPage = buildGoogleNextPage(endpoint, engineId, query, apiKey, nextPage);
                            } else {
                                getRequest = new HttpGet(buildGoogleUrl(endpoint, engineId, query, apiKey, 0));
                                parsedNextPage = buildGoogleUrl(endpoint, engineId, query, apiKey, 10);
                            }
                        } else if ("bing".equalsIgnoreCase(engine)) {
                            if (nextPage != null) {
                                getRequest = new HttpGet(nextPage);
                                parsedNextPage = buildBingNextPage(endpoint, query, nextPage);
                            } else {
                                getRequest = new HttpGet(buildBingUrl(endpoint, query, 0));
                                parsedNextPage = buildBingUrl(endpoint, query, 10);
                            }
                            getRequest.addHeader("Ocp-Apim-Subscription-Key", apiKey);
                        } else {
                            // Search engine not supported.
                            listener.onFailure(new IllegalArgumentException("Unsupported search engine: %s".formatted(engine)));
                        }
                        CloseableHttpResponse res = httpClient.execute(getRequest);
                        if (res.getCode() >= HttpStatus.SC_BAD_REQUEST) {
                            listener
                                .onFailure(
                                    new IllegalArgumentException("Web search failed: %d %s".formatted(res.getCode(), res.getReasonPhrase()))
                                );
                        } else {
                            String responseString = EntityUtils.toString(res.getEntity());
                            parseResponse(responseString, parsedNextPage, token, engine, listener);
                        }
                    }
                    return null;
                });
            } catch (Exception e) {
                listener.onFailure(new IllegalStateException("Web search failed: %s".formatted(e.getMessage())));
            }
        });
    }

    private String buildDDGEndpoint(String endpoint, String query) {
        return "%s?q=%s".formatted(endpoint, query);
    }

    private String buildGoogleNextPage(String endpoint, String engineId, String query, String apiKey, String currentPage) {
        String[] offsetSplit = currentPage.split("&start=");
        int offset = NumberUtils.toInt(offsetSplit[1], 0) + 10;
        return buildGoogleUrl(endpoint, engineId, query, apiKey, offset);
    }

    private String buildGoogleUrl(String endpoint, String engineId, String query, String apiKey, int start) {
        return "%s?q=%s&cx=%s&key=%s&start=%d".formatted(endpoint, query, engineId, apiKey, start);
    }

    private String buildBingNextPage(String endpoint, String query, String currentPage) {
        String[] offsetSplit = currentPage.split("&offset=");
        int offset = NumberUtils.toInt(offsetSplit[1], 0) + 10;
        return buildBingUrl(endpoint, query, offset);
    }

    private String getDefaultEndpoint(String engine) {
        return switch (engine.toLowerCase(Locale.ROOT)) {
            case "google" -> "https://customsearch.googleapis.com/customsearch/v1";
            case "bing" -> "https://api.bing.microsoft.com/v7.0/search";
            case "duckduckgo" -> "https://duckduckgo.com/html";
            default -> throw new IllegalArgumentException("Unsupported search engine: %s".formatted(engine));
        };
    }

    // pagination: https://learn.microsoft.com/en-us/bing/search-apis/bing-web-search/page-results#paging-through-search-results
    private String buildBingUrl(String endpoint, String query, int offset) {
        return "%s?q%s&textFormat=HTML&count=10&offset=%d".formatted(endpoint, query, offset);
    }

    private <T> void parseResponse(String rawResponse, String nextPage, String token, String engine, ActionListener<T> listener) {
        JsonObject rawJson = JsonParser.parseString(rawResponse).getAsJsonObject();
        switch (engine.toLowerCase(Locale.ROOT)) {
            case "google":
                parseGoogleResults(rawJson, nextPage, token, listener);
                break;
            case "bing":
                parseBingResults(rawJson, nextPage, token, listener);
            default:
                listener.onFailure(new RuntimeException("Unsupported search engine: %s".formatted(engine)));
        }
    }

    private <T> void parseGoogleResults(JsonObject googleResponse, String nextPage, String token, ActionListener<T> listener) {
        Map<String, Object> results = new HashMap<>();
        results.put("next_page", nextPage);
        // extract search results, each item is a search result:
        // https://developers.google.com/custom-search/v1/reference/rest/v1/Search#result
        JsonArray items = googleResponse.getAsJsonArray("items");
        List<Map<String, String>> crawlResults = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.get(i).getAsJsonObject();
            // extract the actual link for scrawl.
            String link = item.get("link").getAsString();
            // extract title and content.
            Map<String, String> crawlResult = crawlPage(link, token);
            crawlResults.add(crawlResult);
        }
        results.put("items", crawlResults);
        listener.onResponse((T) StringUtils.gson.toJson(results));
    }

    private <T> void parseBingResults(JsonObject bingResponse, String nextPage, String token, ActionListener<T> listener) {
        Map<String, Object> results = new HashMap<>();
        results.put("next_page", nextPage);
        List<Map<String, String>> crawlResults = new ArrayList<>();
        JsonArray values = bingResponse.get("webPages").getAsJsonObject().getAsJsonArray("value");
        for (int i = 0; i < values.size(); i++) {
            JsonObject value = values.get(i).getAsJsonObject();
            String link = value.get("url").getAsString();
            Map<String, String> crawlResult = crawlPage(link, token);
            crawlResults.add(crawlResult);
        }
        results.put("items", crawlResults);
        listener.onResponse((T) StringUtils.gson.toJson(results));
    }

    private <T> void fetchDuckDuckGoResult(String endpoint, String token, ActionListener<T> listener) {
        try {
            Document doc = Jsoup.connect(endpoint).timeout(10000).get();
            Optional<Elements> pageResult = Optional
                .of(doc)
                .map(x -> x.getElementById("links"))
                .map(x -> x.getElementsByClass("results_links"));
            if (pageResult.isEmpty()) {
                listener.onFailure(new IllegalStateException("Failed to fetch duckduckgo results!"));
                return;
            }
            String nextPage = getDDGNextPageLink(endpoint, doc);
            Map<String, Object> results = new HashMap<>();
            List<Map<String, String>> crawlResults = new ArrayList<>();
            for (Element result : pageResult.get()) {
                Optional<Element> elementOptional = Optional
                    .of(result)
                    .map(x -> x.getElementsByClass("links_main"))
                    .stream()
                    .findFirst()
                    .map(x -> Objects.requireNonNull(x.first()).getElementsByTag("a").first());
                if (elementOptional.isEmpty()) {
                    listener.onFailure(new IllegalStateException("Failed to fetch duckduckgo results as no valid link element found!"));
                    return;
                }
                String link = elementOptional.get().attr("href");
                Map<String, String> crawlResult = crawlPage(link, token);
                crawlResults.add(crawlResult);
            }
            results.put("next_page", nextPage);
            results.put("items", crawlResults);
            listener.onResponse((T) StringUtils.gson.toJson(results));
        } catch (IOException e) {
            log.error("Failed to fetch duckduckgo results due to exception!");
            listener.onFailure(e);
        }
    }

    private String getDDGNextPageLink(String endpoint, Document doc) {
        Element navLinkDiv = doc.select("div.nav-link").first();
        if (navLinkDiv == null) {
            log.warn("Failed to find next page link div for duckduckgo");
            return null;
        }
        Element form = navLinkDiv.selectFirst("form");
        if (form == null) {
            log.warn("Failed to find next page link form for duckduckgo");
            return null;
        }
        String[] urlAndParams = endpoint.split("\\?q");
        if (urlAndParams.length != 2) {
            log.warn("Failed to find next page link url for duckduckgo");
            return null;
        }
        StringBuilder sb = new StringBuilder(urlAndParams[0]);
        // Get all input elements within the form
        Elements inputs = form.select("input:not([type=submit])");
        for (int i = 0; i < inputs.size(); i++) {
            String name = inputs.get(i).attr("name");
            String value = inputs.get(i).attr("value");
            if ("q".equalsIgnoreCase(name)) {
                value = value.replaceAll(" ", "+");
            }
            if (i == 0) {
                sb.append("?").append(name).append("=").append(value);
            } else {
                sb.append("&").append(name).append("=").append(value);
            }
        }
        return sb.toString();
    }

    /**
     * crawl a page and put the page content into the results map if it can be crawled successfully.
     *
     * @param url The url to crawl
     */
    private Map<String, String> crawlPage(String url, String token) {
        try {
            Connection connection = Jsoup.connect(url).timeout(10000).userAgent(USER_AGENT);
            if (token != null && !token.isEmpty()) {
                connection.header("Authorization", "Bearer %s".formatted(token));
            }
            Document doc = connection.get();
            Elements parentElements = doc.select("body");
            if (isCaptchaOrLoginPage(doc)) {
                log.debug("Skipping {} - CAPTCHA required", url);
                return null;
            }

            Element bodyElement = parentElements.getFirst();
            String title = bodyElement.select("title").text();
            String content = bodyElement.text();
            return ImmutableMap.of("url", url, "title", title, "content", content);
        } catch (Exception e) {
            log.error("Failed to crawl link: {}", url);
            return null;
        }
    }

    private boolean isCaptchaOrLoginPage(Document doc) {
        String html = doc.html().toLowerCase(Locale.ROOT);
        // 1. Check for CAPTCHA indicators
        return !doc.select("input[name*='captcha'], input[id*='captcha']").isEmpty() ||
        // Google reCAPTCHA markers
            !doc.select(".g-recaptcha, div[data-sitekey]").isEmpty() ||
            // CAPTCHA image patterns
            !doc.select("img[src*='captcha'], img[src*='recaptcha']").isEmpty() ||
            // Text-based indicators
            org.apache.commons.lang3.StringUtils.containsIgnoreCase(html, "verify you are human") ||
            // hCAPTCHA detection
            !doc.select(".h-captcha").isEmpty();
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        String engine = parameters.get("engine");
        boolean hasNextPage = parameters.containsKey("next_page") && !parameters.get("next_page").isEmpty();
        boolean hasQuery = parameters.containsKey("query") && !parameters.get("query").isEmpty();
        boolean hasEndpoint = parameters.containsKey("endpoint") && !parameters.get("endpoint").isEmpty();
        if ("google".equalsIgnoreCase(engine)) {
            boolean hasEngineIdAndApiKey = parameters.containsKey("engine_id")
                && !parameters.get("engine_id").isEmpty()
                && parameters.containsKey("api_key")
                && !parameters.get("api_key").isEmpty();
            return (hasEndpoint && hasQuery && hasEngineIdAndApiKey) || hasNextPage;
        } else if ("duckduckgo".equalsIgnoreCase(engine)) {
            return hasQuery || hasNextPage;
        } else if ("bing".equalsIgnoreCase(engine)) {
            boolean hasApiKey = parameters.containsKey("api_key") && !parameters.get("api_key").isEmpty();
            return (hasEndpoint && hasApiKey) || hasNextPage;
        } else {
            log.error("Unsupported search engine: {}", engine);
            return false;
        }
    }

    public static class Factory implements Tool.Factory<WebSearchTool> {
        private static Factory INSTANCE;
        private ThreadPool threadPool;

        public static Factory getInstance() {
            if (INSTANCE == null) {
                synchronized (WebSearchTool.class) {
                    if (INSTANCE == null) {
                        INSTANCE = new Factory();
                    }
                }
            }
            return INSTANCE;
        }

        public void init(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public WebSearchTool create(Map<String, Object> map) {
            return new WebSearchTool(threadPool);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return "1.0";
        }
    }
}
