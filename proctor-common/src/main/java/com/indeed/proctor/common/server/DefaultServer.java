package com.indeed.proctor.common.server;

import com.duowan.sysop.hawk.metrics.client2.type.DefMetricsValue;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.indeed.proctor.common.*;
import com.indeed.proctor.common.admin.model.Test;
import com.indeed.proctor.common.model.Audit;
import com.indeed.proctor.common.model.ConsumableTestDefinition;
import com.indeed.proctor.common.model.TestBucket;
import com.indeed.proctor.common.model.TestType;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.streams.Pump;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class DefaultServer extends AbstractVerticle {
    private static final Logger LOGGER = Logger.getLogger(DefaultServer.class);

    private static final String FALLBACK_TEST_MATRIX = "/data/yy/vxlog/abtesting/test-matrix.json";

    private Proctor proctor;
    private RemoteProctorLoader loader;
    @Nullable
    private String lastLoadErrorMessage = "load never attempted";
    @Nullable
    private AbstractProctorDiffReporter diffReporter = new AbstractProctorDiffReporter();
    private MongoClient mongoClient;
    private HttpClient httpClient;
    private DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private Registry registry;

    // Called when verticle is deployed
    @Override
    public void start() {
        LOGGER.info("start - Verticle `" + DefaultServer.class.getSimpleName() + "` is deployed");

        Handler<Void> then = v -> {
            // 启动时从远程加载实验定义
            startupLoad();

            // 初始化成员
            initMember();

            // 设置API服务
            setupHttpServer();

            // 定时从远程加载实验定义
            periodicLoad();

            // 其它设置工作
            setupOnce();
        };

        // 全局初始化，仅一次
        vertx.sharedData().getCounter("global-once", res -> {
            if (res.succeeded()) {
                Counter counter = res.result();
                counter.getAndIncrement(r -> {
                    if (r.succeeded()) {
                        long c = r.result();
                        if (c == 0) {
                            setupGlobalOnce(then);
                        } else {
                            then.handle(null);
                        }
                    } else {
                        LOGGER.error("Something went wrong!", r.cause());
                    }
                });
            } else {
                LOGGER.error("Something went wrong!", res.cause());
            }
        });
    }

    private void setupGlobalOnce(Handler<Void> then) {
        context.executeBlocking(future -> {
            MetricsClient.setup();
            future.complete();
        }, res -> {
            then.handle(null);
        });
    }

    private void startupLoad() {
        context.executeBlocking(future -> {
            // Call some blocking API that takes a significant amount of time to return
            try {
                loader = RemoteProctorLoader.createInstance();
                Proctor proctor = loader.doLoad();
                future.complete(proctor);
            } catch (Exception e) {
                future.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                proctor = (Proctor) res.result();
                Audit audit = proctor.getArtifact().getAudit();
                LOGGER.info("Successfully loaded new test matrix definition: " +
                        audit.getVersion() + " @ " + audit.getUpdatedDate() + " by " + audit.getUpdatedBy());
            } else {
                LOGGER.error("Something error when loading Proctor", res.cause());
                fallback();
            }
        });
    }

    private void initMember() {
        String mongoOptions = "?connectTimeoutMS=5000&socketTimeoutMS=2000&maxPoolSize=1000";
        JsonObject mongoConfig = new JsonObject()
                //.put("connection_string", "mongodb://abman:w80IgG8ebQq@221.228.107.70:10005,183.36.121.130:10006,61.140.10.115:10003/abtest" + mongoOptions);
                .put("connection_string", "mongodb://172.27.142.6:27017/abtest" + mongoOptions);
        mongoClient = MongoClient.createShared(vertx, mongoConfig);
        formatter.setTimeZone(Audit.DEFAULT_TIMEZONE);
        registry = new MongoRegistry(vertx, context, mongoClient);
        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setConnectTimeout(5000)
                .setIdleTimeout(2);
        httpClient = vertx.createHttpClient(httpClientOptions);
    }

    private void setupHttpServer() {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        setupGeneralRoute(router);
        setupBizRoute(router);
        server.requestHandler(router::accept).listen(10200);
    }

    private void periodicLoad() {
        vertx.setPeriodic(10000, id -> {
            context.executeBlocking(future -> {
                final Proctor newProctor;
                try {
                    newProctor = loader.doLoad();
                } catch (Throwable t) {
                    lastLoadErrorMessage = t.getMessage();
                    future.fail(t);
                    return;
                }
                lastLoadErrorMessage = null;
                if (newProctor == null) {
                    // This should only happen if the versions of the matrix files are the same.
                    future.complete();
                    return;
                }
                if (proctor != null && newProctor != null) {
                    diffReporter.reportProctorDiff(proctor.getArtifact(), newProctor.getArtifact());
                }
                future.complete(newProctor);
            }, res -> {
                if (res.succeeded()) {
                    if (res.result() == null) {
                        return;
                    }
                    proctor = (Proctor) res.result();
                    Audit audit = proctor.getArtifact().getAudit();
                    LOGGER.info("Successfully loaded new test matrix definition: " +
                            audit.getVersion() + " @ " + audit.getUpdated() + " by " + audit.getUpdatedBy());
                } else {
                    LOGGER.error("Unable to reload proctor from " + loader.getSource() + ": " + res.cause().toString());
                }
            });
        });
    }

    private void setupOnce() {
        vertx.sharedData().getCounter("dump-experiments", res -> {
            if (res.succeeded()) {
                Counter counter = res.result();
                counter.getAndIncrement(r -> {
                    if (r.succeeded()) {
                        long c = r.result();
                        if (c == 0) {
                            // 只做一次的setup放这里
                            setupPeriodic();
                            registry.setupPeriodic();
                        }
                    } else {
                        LOGGER.error("Something went wrong!", r.cause());
                    }
                });
            } else {
                LOGGER.error("Something went wrong!", res.cause());
            }
        });
    }

    /**
     * 定时把实验定义转储到文件
     */
    private void setupPeriodic() {
        LOGGER.info("setup periodic to dump experiments to disk");
        vertx.setPeriodic(10000, id -> {
            String dir = Paths.get(".").toAbsolutePath().normalize().toString();
            String file = FALLBACK_TEST_MATRIX;
            context.executeBlocking(future -> {
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file),
                        StandardCharsets.UTF_8))) {
                    proctor.appendTestMatrix(bw);
                    future.complete();
                } catch (IOException e) {
                    future.fail(e);
                }
            }, res -> {
                if (res.failed()) {
                    LOGGER.error("Unable to dump test matrix to file `" + dir + "/" + file + "`", res.cause());
                }
            });
        });
    }

    private void fallback() {
        String dir = Paths.get(".").toAbsolutePath().normalize().toString();
        String file = FALLBACK_TEST_MATRIX;
        context.executeBlocking(future -> {
            final ProctorSpecification specification = new ProctorSpecification();
            specification.setTests(null);
            FileProctorLoader loader = new FileProctorLoader(specification, file);
            try {
                proctor = loader.doLoad();
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                LOGGER.warn("FALLBACK! load test matrix from file `" + dir + "/" + file + "`");
            } else {
                LOGGER.error("Unable to load test matrix from file `" + dir + "/" + file + "`", res.cause());
            }
        });
    }

    private void setupGeneralRoute(Router router) {
        DefMetricsValue metric = MetricsClient.getDefMetricsValue("general");

        // ResponseTimeHandler
        router.route().handler(ctx -> {
            long start = System.currentTimeMillis();
            ctx.addHeadersEndHandler(v -> {
                long duration = System.currentTimeMillis() - start;
                ctx.response().putHeader("x-response-time", duration + "ms");
                metric.markDurationAndCode(duration, ctx.response().getStatusCode());
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
            metric.markCode(500);
        });

        // Add default headers
        router.route().handler(ctx -> {
            ctx.addHeadersEndHandler(v -> {
                ctx.response().putHeader("content-type", "application/json; charset=utf-8");
            });
            ctx.next();
        });
    }

    private void setupBizRoute(Router router) {
        router.get("/").handler(routingContext -> {
            routingContext.response().end("ok");
        });

        router.get("/convert").handler(routingContext -> {
            DefMetricsValue metric = MetricsClient.getDefMetricsValue(routingContext);
            HttpServerRequest request = routingContext.request();
            HttpServerResponse response = routingContext.response();

            // 处理参数
            String userId = Strings.nullToEmpty(request.getParam("userId"));
            String deviceId = Strings.nullToEmpty(request.getParam("deviceId"));

            // 取inputContext
            InputContexts.get(httpClient, mongoClient, userId, deviceId, inputContext -> {

                // 取上次状态
                registry.get(userId, deviceId, lastResult -> {

                    // 哈希分配
                    ProctorResult proctorResult = determineTestBucketMap(userId, deviceId, inputContext);

                    // 合并结果
                    combine(proctorResult, lastResult);

                    // 转换结果
                    JsonObject jsonResponse = toJson(proctorResult);

                    response.endHandler(v -> {

                        // 记录状态
                        registry.update(userId, deviceId, proctorResult);

                        // 保存日志
                        //saveLog(userId, deviceId, jsonResponse);
                    });

                    // 发送应答
                    response.end(jsonResponse.encode());

                    // 记录总成功数
                    metric.markCode(200);
                });
            });
        });

        router.get("/_source").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();
            response.setChunked(true);
            String source = loader.getInputURL().toString();
            httpClient.getAbs(source, reader -> {
                Pump.pump(reader, response, 8192).start();
                reader.endHandler(v -> response.end());
            }).exceptionHandler(t -> {
                response.end(source + ": " + t.toString());
            }).end();
        });
    }

    /**
     * 把 lastResult 合并到 proctorResult
     * @param proctorResult 当前实验结果，合并之后，会被改变
     * @param lastResult 上次实验结果，只读
     */
    private void combine(ProctorResult proctorResult, ProctorResult lastResult) {
        if (lastResult == null)
            return;

        lastResult.getBuckets().keySet().stream()
                // 挑出上次结果里，已暂停的实验
                .filter(testId -> {
                    ConsumableTestDefinition testDefinition = proctor.getTestDefinition(testId);
                    if (testDefinition != null) {
                        return testDefinition.getState().equals(Test.STATE_PAUSED);
                    } else {
                        return false;
                    }
                })
                // 给当前实验结果（必须不包含暂停实验），增加已暂停的实验
                .forEach(testId -> {
                    TestBucket nonExist = proctorResult.getBuckets().put(testId, lastResult.getBuckets().get(testId));
                    if (nonExist != null) {
                        LOGGER.error("A paused experiment `" + testId + "` is present");
                    }
                });
    }

    private void saveLog(String userId, String deviceId, JsonObject json) {
        json.put("request", new JsonObject()
                .put("userId", userId)
                .put("deviceId", deviceId)
        );
        json.put("timestamp", formatter.format(new Date()));
        String collection = "converts";
        mongoClient.save(collection, json, res -> {
            if (res.failed()) {
                LOGGER.error("Unable save to collection " + collection + ": " + res.cause().toString());
            }
        });
    }

    /**
     * 利用metrics需要记录每个实验每个分组的请求数
     */
    private JsonObject toJson(ProctorResult proctorResult) {
        Map<String, String> data = Maps.newHashMap();
        Map<String, TestBucket> buckets = proctorResult.getBuckets();
        for (Map.Entry<String, TestBucket> entry : buckets.entrySet()) {
            String testId = entry.getKey();
            String variationKey = entry.getValue().getName();
            if (variationKey.equals("inactive"))
                continue;
            data.put(testId, variationKey);

            String key = testId + "/" + variationKey;
            MetricsClient.getDefMetricsValue(key).markCode(0);
        }
        JsonObject json = new JsonObject();
        json.put("code", 0);  // 成功
        json.put("message", "Success");
        json.put("data", data);
        return json;
    }

    private ProctorResult determineTestBucketMap(String userId, String deviceId, Map<String, Object> inputContext) {
        Identifiers identifiers = Identifiers.of(
                TestType.USER_ID, userId,
                TestType.DEVICE_ID, deviceId);
        //Map<String, Object> inputContext = Maps.newHashMap();
        Map<String, Integer> forcedGroups = Collections.<String, Integer>emptyMap();
        Collection<String> testNameFilter = Collections.<String>emptyList();
        ProctorResult result = proctor.determineTestGroups(identifiers, inputContext, forcedGroups, testNameFilter);
        return result;
    }
}
