package io.github.singha105.pinotrt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Thin client for the Pinot broker's {@code POST /query/sql} and a few controller admin calls. */
final class PinotClient {

    /** The parts of a broker response this project reports on. */
    record QueryResult(
            int httpStatus,
            String rowsJson,
            List<String> exceptions,
            long timeUsedMs,
            long numDocsScanned,
            long totalDocs,
            long numEntriesScannedInFilter,
            long numEntriesScannedPostFilter,
            long numSegmentsQueried,
            long numConsumingSegmentsQueried) {

        boolean ok() {
            return httpStatus == 200 && exceptions.isEmpty();
        }

        /** First cell of the first row, for single-value queries such as COUNT(*). */
        long firstLong(ObjectMapper mapper) throws IOException {
            JsonNode rows = mapper.readTree(rowsJson);
            if (!rows.isArray() || rows.isEmpty()) {
                throw new IOException("query returned no rows");
            }
            return rows.get(0).get(0).asLong();
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpClient http;
    private final URI queryUri;
    private final Duration timeout;

    PinotClient(String brokerUrl, Duration timeout) {
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.queryUri = URI.create(stripSlash(brokerUrl) + "/query/sql");
        this.timeout = timeout;
    }

    /** Returns the raw response so callers can timestamp it before spending time on JSON parsing. */
    CompletableFuture<HttpResponse<String>> sendAsync(String sql) {
        return http.sendAsync(queryRequest(sql), HttpResponse.BodyHandlers.ofString());
    }

    QueryResult query(String sql) throws IOException, InterruptedException {
        HttpResponse<String> resp = http.send(queryRequest(sql), HttpResponse.BodyHandlers.ofString());
        return parse(resp.statusCode(), resp.body());
    }

    /** POSTs to a controller endpoint and returns the parsed JSON body; non-2xx responses throw. */
    JsonNode controllerPost(String controllerUrl, String path) throws IOException, InterruptedException {
        return controllerCall(HttpRequest.newBuilder(URI.create(stripSlash(controllerUrl) + path))
                .POST(HttpRequest.BodyPublishers.noBody()));
    }

    JsonNode controllerGet(String controllerUrl, String path) throws IOException, InterruptedException {
        return controllerCall(HttpRequest.newBuilder(URI.create(stripSlash(controllerUrl) + path)).GET());
    }

    private JsonNode controllerCall(HttpRequest.Builder builder) throws IOException, InterruptedException {
        HttpResponse<String> resp = http.send(
                builder.timeout(timeout).header("Accept", "application/json").build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IOException("controller returned HTTP " + resp.statusCode() + ": " + resp.body());
        }
        return MAPPER.readTree(resp.body());
    }

    private HttpRequest queryRequest(String sql) {
        ObjectNode body = MAPPER.createObjectNode().put("sql", sql);
        return HttpRequest.newBuilder(queryUri)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();
    }

    static QueryResult parse(int status, String body) {
        List<String> exceptions = new ArrayList<>();
        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (IOException e) {
            exceptions.add("unparseable response: " + e.getMessage());
            return new QueryResult(status, "[]", exceptions, -1, -1, -1, -1, -1, -1, -1);
        }
        for (JsonNode ex : root.path("exceptions")) {
            exceptions.add(ex.path("errorCode").asText() + ": " + ex.path("message").asText());
        }
        if (status != 200 && exceptions.isEmpty()) {
            exceptions.add("HTTP " + status);
        }
        return new QueryResult(
                status,
                root.path("resultTable").path("rows").toString(),
                exceptions,
                root.path("timeUsedMs").asLong(-1),
                root.path("numDocsScanned").asLong(-1),
                root.path("totalDocs").asLong(-1),
                root.path("numEntriesScannedInFilter").asLong(-1),
                root.path("numEntriesScannedPostFilter").asLong(-1),
                root.path("numSegmentsQueried").asLong(-1),
                root.path("numConsumingSegmentsQueried").asLong(-1));
    }

    private static String stripSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }
}
