/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.agent.tools;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.jayway.jsonpath.JsonPath;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.ml.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
@Setter
@Getter
@ToolAnnotation(WebSearchTool.TYPE)
public class WebSearchTool implements Tool {

    public static final String TYPE = "WebSearchTool";

    private static final String DEFAULT_DESCRIPTION =
            "A generic web search tool that supports multiple search engines and endpoints. " +
                    "Parameters: {\"query\": \"search terms\", \"engine\": \"google|duckduckgo|custom\", " +
                    "\"endpoint\": \"API endpoint\", \"params\": {additional engine-specific parameters}}";

    private static final List<String> ELEMENTS_TO_EXTRACT = ImmutableList.of("title", "text");
    private static final String USER_AGENT = "OpenSearchWebCrawler/1.0";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;
    private CloseableHttpClient httpClient;

    public WebSearchTool() {
        this.httpClient = HttpClients.createDefault();
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        try {
            String query = parameters.getOrDefault("query", "");
            String engine = parameters.getOrDefault("engine", "google");
            String endpoint = parameters.getOrDefault("endpoint", getDefaultEndpoint(engine));
            //Google search parameters
            String apiKey = parameters.getOrDefault("api_key", "");
            String engineId = parameters.getOrDefault("engine_id", "");
            String nextPage = parameters.getOrDefault("next_page", "");
            //DuckDuckGo search parameters
            String country = parameters.getOrDefault("country", "us-en");
            String serpapiPagination = parameters.getOrDefault("serpapi_pagination", "");
            //Custom search parameters
            String extraParams = parameters.getOrDefault("custom_params", "");
            String responseJsonPath = parameters.getOrDefault("response_json_path", "$.items.link");
            //Token information of the target link
            String token = parameters.getOrDefault("token", "");

            String searchUrl = "";
            if (engine.equals("google")) {
                //First try with nextPage.
                String start = parseNextPageIndex(nextPage);
                searchUrl = buildRequestUrl4Google(endpoint, engineId, query, apiKey, start);
            } else if (engine.equals("duckduckgo")) {
                // build search url for duckduckgo.
                if (serpapiPagination != null) {
                    searchUrl = JsonParser.parseString(serpapiPagination).getAsJsonObject().get("next").getAsString();
                } else {
                    searchUrl = buildRequestUrl4DuckDuckGo(endpoint, query, apiKey, country);
                }
            } else {
                String start = parseNextPageIndex(nextPage);
                searchUrl = buildRequestUrl4Custom(endpoint, query, start, extraParams);
            }

            HttpGet request = new HttpGet(searchUrl);
            String response = httpClient.execute(request, httpResponse ->
                    EntityUtils.toString(httpResponse.getEntity()));
            normalizeResponse(response, token, engine, responseJsonPath, listener);
        } catch (Exception e) {
            listener.onFailure(new RuntimeException("Web search failed: " + e.getMessage()));
        }
    }

    private String parseNextPageIndex(String nextPage) {
        if (nextPage == null || nextPage.isEmpty()) {
            return "0";
        } else {
            JsonObject nextPageMap = JsonParser.parseString(nextPage).getAsJsonObject();
            return nextPageMap.get("startIndex").getAsString();
        }
    }

    private String getDefaultEndpoint(String engine) {
        return switch (engine.toLowerCase()) {
            case "google" -> "https://customsearch.googleapis.com/customsearch/v1";
            case "bing" -> "https://api.bing.microsoft.com/v7.0/search";
            case "duckduckgo" -> "https://serpapi.com/search.json";
            case "serpapi" -> "https://serpapi.com/search.json";
            case "custom" -> null;
            default -> throw new IllegalArgumentException("Unsupported search engine: " + engine);
        };
    }

    private String buildRequestUrl4Google(String endpoint, String engineId, String query, String apiKey, String start) {
        return endpoint + "?q=" + query +
                "&cx=" + engineId +
                "&key=" + apiKey +
                "&start=" + start;
    }

    private String buildRequestUrl4DuckDuckGo(String endpoint, String query, String apiKey, String country) {
        return endpoint + "?q=" + query +
                "&engine=duckduckgo"  +
                "&kl=" + country +
                "api_key" + apiKey;
    }

    private String buildRequestUrl4Custom(String endpoint, String query, String start, String extraParams) {
        return endpoint + "?q=" + query +
                "start=" + start +
                "&" + extraParams;
    }

    private <T> void normalizeResponse(String rawResponse, String token, String engine, String responseJsonPath, ActionListener<T> listener) {
        JsonObject rawJson = JsonParser.parseString(rawResponse).getAsJsonObject();
        switch (engine.toLowerCase()) {
            case "google":
                parseGoogleResults(rawJson, token, listener);
                break;
            case "duckduckgo":
                parseDuckDuckGoResults(rawJson, token, listener);
                break;
            case "custom":
                parseCustomResults(rawJson, token, responseJsonPath, listener);
            default:
                listener.onFailure(new RuntimeException("Unsupported search engine: " + engine));
        }
    }

    private <T> void parseCustomResults(JsonObject rawJson, String token, String responseJsonPath, ActionListener<T> listener) {
        List<String> link = JsonPath.read(rawJson, responseJsonPath);
        List<Map<String, String>> crawlResults = new ArrayList<>();
        for (String url : link) {
            Map<String, String> crawlResult = crawlPage(url, token);
            if (crawlResult != null) {
                crawlResults.add(crawlResult);
            }
        }
        Map<String, Object> results = new HashMap<>();
        results.put("items", crawlResults);
        listener.onResponse((T) StringUtils.gson.toJson(results));
    }

    private <T> void parseGoogleResults(JsonObject googleResponse, String token, ActionListener<T> listener) {
        Map<String, Object> results = new HashMap<>();
        //extract search page infos, including previous page and next page.
        JsonObject queries = googleResponse.getAsJsonObject("queries");
        results.put("queries", queries);
        //extract search results, each item is a search result: https://developers.google.com/custom-search/v1/reference/rest/v1/Search#result
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

    //TODO add parse duckduckgo result.
    private <T> void parseDuckDuckGoResults(JsonObject ddgResponse, String token, ActionListener<T> listener) {
        JsonObject results = new JsonObject();
        // Add actual parsing logic here
        listener.onResponse((T) results.toString());
    }

    /**
     * crawl a page and put the page content into the results map if it can be crawled successfully.
     * @param url
     */
    private Map<String, String> crawlPage(String url, String token) {
        try {
             Connection connection = Jsoup.connect(url)
                    .timeout(10000)
                    .userAgent(USER_AGENT);
            if (token != null && !token.isEmpty()) {
                connection.header("Authorization", "Bearer " + token);
            }
            Document doc = connection.get();
            Elements parentElements = doc.select("body");
            if (isCaptchaOrLoginPage(doc)) {
                log.debug("Skipping {} - CAPTCHA or login required", url);
                return null;
            }

            Element bodyElement = parentElements.getFirst();
            String title = bodyElement.select("title").text();
            String content = bodyElement.text();
            return ImmutableMap.of(
                    "url", url,
                    "title", title,
                    "content", content);
        } catch (Exception e) {
            log.error("Failed to crawl link: {}", url);
            return null;
        }
    }

    private boolean isCaptchaOrLoginPage(Document doc) {
        String html = doc.html().toLowerCase();
        // 1. Check for CAPTCHA indicators
        boolean hasCaptcha =
                // Common CAPTCHA input fields
                !doc.select("input[name*='captcha'], input[id*='captcha']").isEmpty() ||
                        // Google reCAPTCHA markers
                        !doc.select(".g-recaptcha, div[data-sitekey]").isEmpty() ||
                        // CAPTCHA image patterns
                        !doc.select("img[src*='captcha'], img[src*='recaptcha']").isEmpty() ||
                        // Text-based indicators
                        html.contains("captcha") ||
                        html.contains("verify you are human") ||
                        // hCAPTCHA detection
                        !doc.select(".h-captcha").isEmpty();

        // 2. Check for challenge pages (Cloudflare, etc.)
        boolean hasChallenge =
                html.contains("challenge-platform") ||
                        !doc.select("div#cf-challenge-wrapper").isEmpty() ||
                        html.contains("ddos-protection") ||
                        html.contains("enable javascript") ||
                        html.contains("javascript challenge");
        return hasCaptcha || hasChallenge;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        String engine = parameters.get("engine");
        if ("google".equalsIgnoreCase(engine)) {
            boolean hasNextPage = parameters.containsKey("next_page") &&
                    !parameters.get("next_page").isEmpty();
            boolean firstQuery = parameters.containsKey("query") &&
                    !parameters.get("query").isEmpty() &&
                    parameters.containsKey("engine_id") &&
                    !parameters.get("engine_id").isEmpty() &&
                    parameters.containsKey("api_key") &&
                    !parameters.get("api_key").isEmpty();
            return firstQuery || hasNextPage;
        } else if ("duckduckgo".equalsIgnoreCase(engine)) {
            boolean firstQuery = parameters.containsKey("query") &&
                    !parameters.get("query").isEmpty() &&
                    parameters.containsKey("api_key") &&
                    !parameters.get("api_key").isEmpty();
            boolean hasSerpapiPagination = parameters.containsKey("serpapi_pagination") &&
                    !parameters.get("serpapi_pagination").isEmpty();
            return firstQuery || hasSerpapiPagination;
        } else {
            boolean firstQuery = parameters.containsKey("query") &&
                    !parameters.get("query").isEmpty() &&
                    parameters.containsKey("endpoint") &&
                    !parameters.get("endpoint").isEmpty();
            boolean hasNextPage = parameters.containsKey("next_page") &&
                    !parameters.get("next_page").isEmpty();
            return firstQuery || hasNextPage;
        }
    }

    public static class Factory implements Tool.Factory<WebSearchTool> {
        private static Factory INSTANCE;

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

        @Override
        public WebSearchTool create(Map<String, Object> map) {
            validateParameters(map);
            return new WebSearchTool();
        }

        private void validateParameters(Map<String, Object> map) {
            if (!map.containsKey("engine")) {
                throw new IllegalArgumentException("WebSearchTool requires 'engine' parameter");
            }
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
