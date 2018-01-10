package com.indeed.proctor.common.server;

import com.google.common.collect.Maps;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

import java.util.Map;

public class InputContexts {
    private static final Logger LOGGER = Logger.getLogger(InputContexts.class);

    public static final String isBrandNewUser = "isBrandNewUser";

    public static void get(HttpClient httpClient,
                           MongoClient mongoClient,
                           String userId, String deviceId,
                           Handler<Map<String, Object>> resultHandler) {
        Map<String, Object> inputContext = Maps.newHashMap();
        inputContext.put("userId", userId);
        inputContext.put("deviceId", deviceId);
        inputContext.put(isBrandNewUser, false);

        // 去海度查询设备最后登录的时间
        String hiidoServiceURL = "http://szhiidocosevice.yy.com/service/read";
        httpClient
                .postAbs(hiidoServiceURL, response -> {
                    int staticCode = response.statusCode();
                    if (staticCode != 200) {
                        LOGGER.error("post to `" + hiidoServiceURL + "` got " + staticCode);
                        resultHandler.handle(inputContext);
                        return;
                    }
                    response.bodyHandler(body -> {
                        JsonObject result = new JsonObject(body);
                        JsonArray data = result.getJsonArray("data", new JsonArray());
                        if (data.size() > 0 && data.getJsonObject(0).containsKey("dt")) {
                            LOGGER.debug("device `" + deviceId + "` -> dt `" + data.getJsonObject(0).getString("dt") + "`");
                            resultHandler.handle(inputContext);
                            return;
                        }
                        LOGGER.debug("device `" + deviceId + "` -> not present in hiido, query locally...");
                        JsonObject query = new JsonObject().put("_id", deviceId);
                        JsonObject fields = new JsonObject().put("data", 1);
                        mongoClient.findOne("registry", query, fields,
                                res -> {
                                    if (res.succeeded() && res.result() != null) {
                                        inputContext.put(isBrandNewUser, false);
                                    } else {
                                        inputContext.put(isBrandNewUser, true);
                                    }
                                    resultHandler.handle(inputContext);
                                });
                    }).exceptionHandler(t -> {
                        LOGGER.error("parse response from `" + hiidoServiceURL + "` got " + t.toString());
                        resultHandler.handle(inputContext);
                    });
                })
                .putHeader("content-type", "text/plain")
                .exceptionHandler(t -> {
                    LOGGER.error("post to `" + hiidoServiceURL + "` got " + t.toString());
                    resultHandler.handle(inputContext);
                })
                .end(new JsonObject()
                        .put("reqnum", 0)
                        .put("startindex", 0)
                        .putNull("requestColumns")
                        .put("v", "0.1")
                        .put("appId", "")
                        .put("appKey", "")
                        .put("serviceTypeKey", "hdid_total_original_180")
                        .put("params", new JsonObject()
                                .put("hdid", deviceId))
                        .encode());
    }

    public static void main(final String[] args) {
        Vertx vertx = Vertx.vertx();
        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setConnectTimeout(2000)
                .setIdleTimeout(2000);
        HttpClient httpClient = vertx.createHttpClient(httpClientOptions);
        JsonObject mongoConfig = new JsonObject().put("connection_string", "mongodb://172.27.142.6:27017/abtest");
        MongoClient mongoClient = MongoClient.createShared(vertx, mongoConfig);
        Handler<Map<String, Object>> h = res -> {
            LOGGER.info("device `" + res.get("deviceId") + "` -> " + res.get(isBrandNewUser));
        };

        get(httpClient, mongoClient, "", "", h);
        get(httpClient, mongoClient, "", "0", h);
        get(httpClient, mongoClient, "", "6c2ca739d48197332e2fef404811775acbc7ead6", h);
        get(httpClient, mongoClient, "", "0768c3e8b7372379c70e26bd7ebb5c75db8303d3", h);
        get(httpClient, mongoClient, "", "881abe546d6743c8a894ede986673620", h);
        get(httpClient, mongoClient, "", "1234567", h);
        get(httpClient, mongoClient, "", "123456", h);
    }
}
