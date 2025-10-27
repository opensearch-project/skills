/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import static org.opensearch.ml.common.CommonValue.TOOL_INPUT_SCHEMA_FIELD;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.core5.http.HttpStatus;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.agent.ToolPlugin;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.ml.common.httpclient.MLHttpClientFactory;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.utils.StringUtils;
import org.opensearch.ml.common.utils.ToolUtils;
import org.opensearch.threadpool.ThreadPool;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.JsonPath;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.core.internal.http.async.SimpleHttpContentPublisher;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpResponseHandler;

@Log4j2
@Setter
@Getter
@ToolAnnotation(WebSearchTool.TYPE)
public class WebSearchTool implements Tool {

    public static final String TYPE = "WebSearchTool";

    public static final String DEFAULT_DESCRIPTION =
        "This tool performs a web search using the specified query or fetches the next page of a previous search. "
            + "It accepts one mandatory argument: `query`, which is a search term used to initiate a new search, "
            + "and one optional argument: `next_page`, which is a link to retrieve the next set of search results from a previous response. "
            + "The tool returns the raw documents retrieved from the search engine, along with a `next_page` field for pagination.";

    private static final String USER_AGENT = "OpenSearchWebCrawler/1.0";
    public static final String DEFAULT_INPUT_SCHEMA = "{"
        + "\"type\":\"object\","
        + "\"properties\":{"
        + "\"query\":{"
        + "\"type\":\"string\","
        + "\"description\":\"The search term to query using the configured search engine. This is the primary input used to perform the search.\""
        + "},"
        + "\"next_page\":{"
        + "\"type\":\"string\","
        + "\"description\":\"URL to the next page of search results. If provided, the tool will fetch and return results from this page instead of executing a new search query.\""
        + "}"
        + "},"
        + "\"required\":[\"query\"]"
        + "}";

    public static final Map<String, Object> DEFAULT_ATTRIBUTES = Map.of(TOOL_INPUT_SCHEMA_FIELD, DEFAULT_INPUT_SCHEMA, "strict", false);
    public static final String NEXT_PAGE = "next_page";
    public static final String ENGINE_ID = "engine_id";
    public static final String OFFSET = "offset";
    public static final String DUCKDUCKGO = "duckduckgo";
    public static final String GOOGLE = "google";
    public static final String BING = "bing";
    public static final String CUSTOM = "custom";
    public static final String ITEMS = "items";
    public static final String ENGINE = "engine";
    public static final String ENDPOINT = "endpoint";
    public static final String API_KEY = "api_key";
    public static final String CUSTOM_API = "custom_api";
    public static final String AUTHORIZATION = "Authorization";
    public static final String TITLE = "title";
    public static final String URL = "url";
    public static final String CONTENT = "content";
    public static final String QUERY = "query";
    public static final String QUESTION = "question";
    public static final String QUERY_KEY = "query_key";
    public static final String LIMIT_KEY = "limit_key";
    public static final String CUSTOM_RES_URL_JSONPATH = "custom_res_url_jsonpath";
    public static final String START = "start";

    @Setter
    @Getter
    private String name = TYPE;
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    @Getter
    private String version;
    private final SdkAsyncHttpClient httpClient;

    private final ThreadPool threadPool;
    private Map<String, Object> attributes;

    public WebSearchTool(ThreadPool threadPool) {
        // Use 1s for connection timeout, 3s for read timeout, 30 for max connections of httpclient.
        // For WebSearchTool, we don't allow user to connect to private ip.
        this.httpClient = MLHttpClientFactory.getAsyncHttpClient(Duration.ofSeconds(1), Duration.ofSeconds(3), 30, false);
        this.threadPool = threadPool;
        this.attributes = new HashMap<>();
        attributes.put(TOOL_INPUT_SCHEMA_FIELD, DEFAULT_INPUT_SCHEMA);
        attributes.put("strict", false);
    }

    @Override
    public <T> void run(Map<String, String> originalParameters, ActionListener<T> listener) {
        try {
            Map<String, String> parameters = ToolUtils.extractInputParameters(originalParameters, attributes);
            // common search parameters
            String query = parameters.getOrDefault(QUERY, parameters.get(QUESTION)).replaceAll(" ", "+");
            String engine = parameters.getOrDefault(ENGINE, GOOGLE);
            String endpoint = parameters.getOrDefault(ENDPOINT, getDefaultEndpoint(engine));
            String apiKey = parameters.get(API_KEY);
            String nextPage = parameters.get(NEXT_PAGE);

            // Google search parameters
            String engineId = parameters.get(ENGINE_ID);

            // Custom search parameters
            String authorization = parameters.get(AUTHORIZATION);
            String queryKey = parameters.getOrDefault(QUERY_KEY, "q");
            String offsetKey = parameters.getOrDefault(OFFSET + "_key", OFFSET);
            String limitKey = parameters.getOrDefault(LIMIT_KEY, "limit");
            String customResUrlJsonpath = parameters.get(CUSTOM_RES_URL_JSONPATH);

            threadPool.executor(ToolPlugin.WEBSEARCH_CRAWLER_THREADPOOL).submit(() -> {
                String parsedNextPage;
                if (DUCKDUCKGO.equalsIgnoreCase(engine)) {
                    // duckduckgo has different approach to other APIs as it's not a standard public API.
                    if (nextPage != null) {
                        fetchDuckDuckGoResult(nextPage, listener);
                    } else {
                        fetchDuckDuckGoResult(buildDDGEndpoint(getDefaultEndpoint(engine), query), listener);
                    }
                } else {
                    SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder().method(SdkHttpMethod.GET);
                    if (GOOGLE.equalsIgnoreCase(engine)) {
                        if (nextPage != null) {
                            builder.uri(nextPage);
                            parsedNextPage = buildGoogleNextPage(endpoint, engineId, query, apiKey, nextPage);
                        } else {
                            builder.uri(buildGoogleUrl(endpoint, engineId, query, apiKey, 0));
                            parsedNextPage = buildGoogleUrl(endpoint, engineId, query, apiKey, 10);
                        }
                    } else if (BING.equalsIgnoreCase(engine)) {
                        if (nextPage != null) {
                            builder.uri(nextPage);
                            parsedNextPage = buildBingNextPage(endpoint, query, nextPage);
                        } else {
                            builder.uri(buildBingUrl(endpoint, query, 0));
                            parsedNextPage = buildBingUrl(endpoint, query, 10);
                        }
                        builder.putHeader("Ocp-Apim-Subscription-Key", apiKey);
                    } else if (CUSTOM.equalsIgnoreCase(engine)) {
                        if (nextPage != null) {
                            builder.uri(nextPage);
                            parsedNextPage = buildCustomNextPage(endpoint, nextPage, queryKey, query, offsetKey, limitKey);
                        } else {
                            builder.uri(buildCustomUrl(endpoint, queryKey, query, offsetKey, 0, limitKey));
                            parsedNextPage = buildCustomUrl(endpoint, queryKey, query, offsetKey, 10, limitKey);
                        }
                        builder.putHeader(AUTHORIZATION, authorization);
                    } else {
                        // Search engine not supported.
                        listener
                            .onFailure(new IllegalArgumentException(String.format(Locale.ROOT, "Unsupported search engine: %s", engine)));
                        return;
                    }
                    SdkHttpFullRequest getRequest = builder.build();
                    AsyncExecuteRequest executeRequest = AsyncExecuteRequest
                        .builder()
                        .request(getRequest)
                        .requestContentPublisher(new SimpleHttpContentPublisher(getRequest))
                        .responseHandler(
                            new WebSearchResponseHandler<T>(endpoint, authorization, parsedNextPage, engine, customResUrlJsonpath, listener)
                        )
                        .build();
                    try {
                        httpClient.execute(executeRequest);
                    } catch (Exception e) {
                        log.error("Web search failed!", e);
                        listener.onFailure(new IllegalStateException(String.format(Locale.ROOT, "Web search failed: %s", e.getMessage())));
                    }
                }
            });
        } catch (Exception e) {
            listener.onFailure(new IllegalStateException(String.format(Locale.ROOT, "Web search failed: %s", e.getMessage())));
        }
    }

    private String buildDDGEndpoint(String endpoint, String query) {
        return String.format(Locale.ROOT, "%s?q=%s", endpoint, query);
    }

    private String buildGoogleNextPage(String endpoint, String engineId, String query, String apiKey, String currentPage) {
        String[] offsetSplit = currentPage.split("&" + START + "=");
        int offset = NumberUtils.toInt(offsetSplit[1], 0) + 10;
        return buildGoogleUrl(endpoint, engineId, query, apiKey, offset);
    }

    private String buildGoogleUrl(String endpoint, String engineId, String query, String apiKey, int start) {
        return String.format(Locale.ROOT, "%s?q=%s&cx=%s&key=%s&" + START + "=%d", endpoint, query, engineId, apiKey, start);
    }

    private String buildBingNextPage(String endpoint, String query, String currentPage) {
        String[] offsetSplit = currentPage.split("&" + OFFSET + "=");
        int offset = NumberUtils.toInt(offsetSplit[1], 0) + 10;
        return buildBingUrl(endpoint, query, offset);
    }

    private String buildCustomNextPage(
        String endpoint,
        String currentPage,
        String queryKey,
        String query,
        String offsetKey,
        String limitKey
    ) {
        String[] pageSplit = currentPage.split(String.format(Locale.ROOT, "&%s=", offsetKey));
        int offsetValue = NumberUtils.toInt(pageSplit[1].split("&")[0], 0) + 10;
        return buildCustomUrl(endpoint, queryKey, query, offsetKey, offsetValue, limitKey);
    }

    private String buildCustomUrl(String endpoint, String queryKey, String query, String offsetKey, int offsetValue, String limitKey) {
        return String.format(Locale.ROOT, "%s?%s=%s&%s=%d&%s=10", endpoint, queryKey, query, offsetKey, offsetValue, limitKey);
    }

    private String getDefaultEndpoint(String engine) {
        return switch (engine.toLowerCase(Locale.ROOT)) {
            case GOOGLE -> "https://customsearch.googleapis.com/customsearch/v1";
            case BING -> "https://api.bing.microsoft.com/v7.0/search";
            case DUCKDUCKGO -> "https://duckduckgo.com/html";
            case CUSTOM -> null;
            default -> throw new IllegalArgumentException(String.format(Locale.ROOT, "Unsupported search engine: %s", engine));
        };
    }

    // pagination: https://learn.microsoft.com/en-us/bing/search-apis/bing-web-search/page-results#paging-through-search-results
    private String buildBingUrl(String endpoint, String query, int offset) {
        return String.format(Locale.ROOT, "%s?q%s&textFormat=HTML&count=10&" + OFFSET + "=%d", endpoint, query, offset);
    }

    private <T> void fetchDuckDuckGoResult(String endpoint, ActionListener<T> listener) {
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
                Map<String, String> crawlResult = crawlPage(link, null);
                crawlResults.add(crawlResult);
            }
            results.put(NEXT_PAGE, nextPage);
            results.put(ITEMS, crawlResults);
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

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        String engine = parameters.get(ENGINE);
        if (org.apache.commons.lang3.StringUtils.isEmpty(engine)) {
            return false;
        }

        boolean isQueryEmpty = org.apache.commons.lang3.StringUtils.isEmpty(parameters.getOrDefault(QUERY, parameters.get(QUESTION)));
        if (isQueryEmpty) {
            log.warn("Query is empty");
            return false;
        }

        boolean isEndpointEmpty = org.apache.commons.lang3.StringUtils
            .isEmpty(parameters.getOrDefault(ENDPOINT, getDefaultEndpoint(engine)));
        if (isEndpointEmpty) {
            log.warn("Endpoint is empty");
            return false;
        }

        if (GOOGLE.equalsIgnoreCase(engine)) {
            boolean hasEngineIdAndApiKey = parameters.containsKey(ENGINE_ID)
                && !parameters.get(ENGINE_ID).isEmpty()
                && parameters.containsKey(API_KEY)
                && !parameters.get(API_KEY).isEmpty();
            if (!hasEngineIdAndApiKey) {
                log.warn("Google search" + ENGINE_ID + "or api_key is empty");
                return false;
            }
            return true;
        } else if (DUCKDUCKGO.equalsIgnoreCase(engine)) {
            return true;
        } else if (BING.equalsIgnoreCase(engine)) {
            boolean hasApiKey = org.apache.commons.lang3.StringUtils.isEmpty(parameters.get(API_KEY));
            if (!hasApiKey) {
                log.warn("Bing search api_key is empty");
                return false;
            }
            return true;
        } else if (CUSTOM.equalsIgnoreCase(engine)) {
            String customApi = parameters.get(CUSTOM_API);
            String customResUrlJsonpath = parameters.get(CUSTOM_RES_URL_JSONPATH);
            if (org.apache.commons.lang3.StringUtils.isEmpty(customApi)
                || org.apache.commons.lang3.StringUtils.isEmpty(customResUrlJsonpath)) {
                log.warn("custom search API is empty or result json path is empty");
                return false;
            }

            return true;
        }

        log.error("Unsupported search engine: {}", engine);
        return false;
    }

    /**
     * crawl a page and put the page content into the results map if it can be crawled successfully.
     *
     * @param url The url to crawl
     */
    public Map<String, String> crawlPage(String url, String authorization) {
        try {
            Connection connection = Jsoup.connect(url).timeout(10000).userAgent(USER_AGENT);
            if (authorization != null) {
                connection.header(AUTHORIZATION, authorization);
            }
            Document doc = connection.get();
            Elements parentElements = doc.select("body");
            if (isCaptchaOrLoginPage(doc)) {
                log.debug("Skipping {} - CAPTCHA required", url);
                return null;
            }

            Element bodyElement = parentElements.getFirst();
            String title = bodyElement.select(TITLE).text();
            String content = bodyElement.text();
            return ImmutableMap.of(URL, url, TITLE, title, CONTENT, content);
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

        @Override
        public Map<String, Object> getDefaultAttributes() {
            return DEFAULT_ATTRIBUTES;
        }
    }

    private final class WebSearchResponseHandler<T> implements SdkAsyncHttpResponseHandler {
        private final String endpoint;
        private final String authorization;
        private final String parsedNextPage;
        private final String engine;
        private final String customResUrlJsonpath;
        private final ActionListener<T> listener;

        public WebSearchResponseHandler(
            String endpoint,
            String authorization,
            String parsedNextPage,
            String engine,
            String customResUrlJsonpath,
            ActionListener<T> listener
        ) {
            this.endpoint = endpoint;
            this.authorization = authorization;
            this.parsedNextPage = parsedNextPage;
            this.engine = engine;
            this.customResUrlJsonpath = customResUrlJsonpath;
            this.listener = listener;
        }

        @Override
        public void onHeaders(SdkHttpResponse response) {
            SdkHttpFullResponse sdkResponse = (SdkHttpFullResponse) response;
            log.debug("received response headers: " + sdkResponse.headers());
            int statusCode = sdkResponse.statusCode();
            if (statusCode < HttpStatus.SC_OK || statusCode > HttpStatus.SC_MULTIPLE_CHOICES) {
                log
                    .error(
                        "Received error from endpoint:{} with status code {}, response headers: {}",
                        endpoint,
                        statusCode,
                        sdkResponse.headers()
                    );
                listener
                    .onFailure(
                        new OpenSearchStatusException(
                            String.format(Locale.ROOT, "Failed to fetch results from endpoint: %s", endpoint),
                            RestStatus.fromCode(statusCode)
                        )
                    );
            }
        }

        @Override
        public void onStream(Publisher<ByteBuffer> stream) {
            stream.subscribe(new Subscriber<>() {
                private final StringBuilder responseBuilder = new StringBuilder();
                private Subscription subscription;

                @Override
                public void onSubscribe(Subscription subscription) {
                    log.debug("Starting to fetch response...");
                    this.subscription = subscription;
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer byteBuffer) {
                    responseBuilder.append(StandardCharsets.UTF_8.decode(byteBuffer));
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onError(Throwable throwable) {
                    log.error("Failed to fetch results from endpoint: {}", endpoint, throwable);
                    listener.onFailure(new RuntimeException(throwable));
                }

                @Override
                public void onComplete() {
                    log.debug("Successfully fetched results from endpoint: {}", endpoint);
                    parseResponse(responseBuilder.toString(), authorization, parsedNextPage, engine, customResUrlJsonpath, listener);
                }
            });
        }

        @Override
        public void onError(Throwable error) {
            log.error("Failed to fetch results from endpoint: {}", endpoint, error);
            listener.onFailure(new RuntimeException(error));
        }

        private <T> void parseResponse(
            String rawResponse,
            String authorization,
            String nextPage,
            String engine,
            String customResUrlJsonpath,
            ActionListener<T> listener
        ) {
            JsonObject rawJson = JsonParser.parseString(rawResponse).getAsJsonObject();
            switch (engine.toLowerCase(Locale.ROOT)) {
                case GOOGLE:
                    parseGoogleResults(rawJson, nextPage, listener);
                    break;
                case BING:
                    parseBingResults(rawJson, nextPage, listener);
                    break;
                case CUSTOM:
                    List<String> urls = JsonPath.read(rawResponse, customResUrlJsonpath);
                    parseCustomResults(urls, authorization, nextPage, listener);
                    break;
                default:
                    listener.onFailure(new RuntimeException(String.format(Locale.ROOT, "Unsupported search engine: %s", engine)));
            }
        }

        private <T> void parseGoogleResults(JsonObject googleResponse, String nextPage, ActionListener<T> listener) {
            Map<String, Object> results = new HashMap<>();
            results.put(NEXT_PAGE, nextPage);
            // extract search results, each item is a search result:
            // https://developers.google.com/custom-search/v1/reference/rest/v1/Search#result
            JsonArray items = googleResponse.getAsJsonArray(ITEMS);
            List<Map<String, String>> crawlResults = new ArrayList<>();
            for (int i = 0; i < items.size(); i++) {
                JsonObject item = items.get(i).getAsJsonObject();
                // extract the actual link for scrawl.
                String link = item.get("link").getAsString();
                // extract title and content.
                Map<String, String> crawlResult = crawlPage(link, null);
                crawlResults.add(crawlResult);
            }
            results.put(ITEMS, crawlResults);
            listener.onResponse((T) StringUtils.gson.toJson(results));
        }

        private <T> void parseBingResults(JsonObject bingResponse, String nextPage, ActionListener<T> listener) {
            Map<String, Object> results = new HashMap<>();
            results.put(NEXT_PAGE, nextPage);
            List<Map<String, String>> crawlResults = new ArrayList<>();
            JsonArray values = bingResponse.get("webPages").getAsJsonObject().getAsJsonArray("value");
            for (int i = 0; i < values.size(); i++) {
                JsonObject value = values.get(i).getAsJsonObject();
                String link = value.get(URL).getAsString();
                Map<String, String> crawlResult = crawlPage(link, null);
                crawlResults.add(crawlResult);
            }
            results.put(ITEMS, crawlResults);
            listener.onResponse((T) StringUtils.gson.toJson(results));
        }

        private <T> void parseCustomResults(List<String> urls, String authorization, String nextPage, ActionListener<T> listener) {
            Map<String, Object> results = new HashMap<>();
            results.put(NEXT_PAGE, nextPage);
            List<Map<String, String>> crawlResults = new ArrayList<>();
            for (int i = 0; i < urls.size(); i++) {
                String link = urls.get(i);
                Map<String, String> crawlResult = crawlPage(link, authorization);
                crawlResults.add(crawlResult);
            }
            results.put(ITEMS, crawlResults);
            listener.onResponse((T) StringUtils.gson.toJson(results));
        }
    }
}
