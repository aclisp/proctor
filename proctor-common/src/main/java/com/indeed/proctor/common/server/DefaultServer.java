package com.indeed.proctor.common.server;

import com.google.common.collect.Maps;
import com.indeed.proctor.common.*;
import com.indeed.proctor.common.model.TestBucket;
import com.indeed.proctor.common.model.TestType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class DefaultServer extends AbstractVerticle {
    private static final Logger LOGGER = Logger.getLogger(DefaultServer.class);

    private Proctor proctor;

    // Called when verticle is deployed
    @Override
    public void start() {
        LOGGER.info("start - Verticle DefaultServer is deployed");

        context.executeBlocking(future -> {
            // Call some blocking API that takes a significant amount of time to return
            try {
                RemoteProctorLoader remoteProctorLoader = RemoteProctorLoader.createInstance();
                Proctor proctor = remoteProctorLoader.doLoad();
                future.complete(proctor);
            }
            catch (MalformedURLException e) {
                future.fail(e);
            } catch (MissingTestMatrixException e) {
                future.fail(e);
            } catch (IOException e) {
                future.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                proctor = (Proctor) res.result();
                LOGGER.info("Proctor is loaded and set");
            } else {
                LOGGER.error("Something error when loading Proctor", res.cause());
            }
        });

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        setupGeneralRoute(router);
        setupBizRoute(router);
        server.requestHandler(router::accept).listen(3001);
    }

    private void setupGeneralRoute(Router router) {
        // ResponseTimeHandler
        router.route().handler(ctx -> {
            long start = System.currentTimeMillis();
            ctx.addHeadersEndHandler(v -> {
                long duration = System.currentTimeMillis() - start;
                ctx.response().putHeader("x-response-time", duration + "ms");
            });
            ctx.next();
        });

        router.route().handler(TimeoutHandler.create(60000));
        router.route().handler(BodyHandler.create());
        router.route().handler(CorsHandler.create("*")
                .allowedHeader("Authorization")
                .allowedHeader("Content-Type")
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.PUT)
                .allowedMethod(HttpMethod.OPTIONS)
                .allowedMethod(HttpMethod.DELETE)
                .allowedMethod(HttpMethod.POST));

        router.route().failureHandler(failureRoutingContext -> {
            // Status code will be 500 for the RuntimeException or ??? for the other failure
            HttpServerResponse response = failureRoutingContext.response();
            response.setStatusCode(500).end();
        });

        // Add default headers
        router.route().handler(ctx -> {
            ctx.addHeadersEndHandler(v -> {
                ctx.response().putHeader("content-type", "application/json");
            });
            ctx.next();
        });
    }

    private void setupBizRoute(Router router) {
        router.get("/convert").handler(routingContext -> {
            HttpServerRequest request = routingContext.request();
            HttpServerResponse response = routingContext.response();
            String userId = request.getParam("userId");
            if (userId == null) {
                userId = "0";
            }
            String deviceId = request.getParam("deviceId");
            if (deviceId == null) {
                deviceId = "0";
            }

            Identifiers identifiers = Identifiers.of(
                    TestType.USER_ID, userId,
                    TestType.DEVICE_ID, deviceId);
            Map<String, Object> inputContext = Maps.newHashMap();
            Map<String, Integer> forcedGroups = Collections.<String, Integer>emptyMap();
            Collection<String> testNameFilter = Collections.<String>emptyList();
            ProctorResult result = proctor.determineTestGroups(identifiers, inputContext, forcedGroups, testNameFilter);
            Map<String, TestBucket> buckets = result.getBuckets();
            Map<String, String> data = Maps.newHashMap();
            for (Map.Entry<String, TestBucket> entry : buckets.entrySet()) {
                String testId = entry.getKey();
                String variationKey = entry.getValue().getName();
                if (variationKey.equals("inactive"))
                    continue;
                data.put(testId, variationKey);
            }
            JsonObject json = new JsonObject();
            json.put("code", 0);  // 成功
            json.put("message", "Success");
            json.put("data", data);

            LOGGER.info("convert userId `" + userId + "` deviceId `" + deviceId + "` to " + json.encodePrettily());

            response.end(json.encode());
        });

        router.get("/ShowTestMatrix").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();
            try {
                StringWriter sw = new StringWriter();
                proctor.appendTestMatrix(sw);
                response.end(sw.toString());
            } catch (IOException e) {
                response.end(e.toString());
            }
        });
    }

}
