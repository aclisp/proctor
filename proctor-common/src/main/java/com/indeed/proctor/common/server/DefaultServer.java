package com.indeed.proctor.common.server;

import com.duowan.sysop.hawk.metrics.client2.type.DefMetricsValue;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class DefaultServer extends AbstractVerticle {
    private static final Logger LOGGER = Logger.getLogger(DefaultServer.class);

    private static final String FALLBACK_TEST_MATRIX = Config.FALLBACK_TEST_MATRIX;

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
        String mongoOptions = "?connectTimeoutMS=5000&socketTimeoutMS=2000&maxPoolSize=1000&maxIdleTimeMS=60000&waitQueueMultiple=1";
        JsonObject mongoConfig = new JsonObject()
                .put("connection_string", Config.MONGODB_CONNECTION_STRING + mongoOptions);
        mongoClient = MongoClient.createShared(vertx, mongoConfig);
        formatter.setTimeZone(Audit.DEFAULT_TIMEZONE);
        registry = new MongoRegistry(vertx, context, mongoClient);
        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setConnectTimeout(5000)
                .setIdleTimeout(2)
                .setKeepAlive(true)
                .setMaxPoolSize(1000)
                .setMaxWaitQueueSize(0)
                .setPipelining(false);
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

        router.route().handler(TimeoutHandler.create(60000, ErrorCode.SERVER_PROCESSING_TIMEOUT));
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
            int statusCode = failureRoutingContext.statusCode();
            Throwable failure = failureRoutingContext.failure();
            HttpServerResponse response = failureRoutingContext.response();

            if (failure != null) {
                LOGGER.error("Got exception when handling route", failure);
                response.setStatusCode(ErrorCode.INTERNAL_SERVER_ERROR).end();
            } else {
                LOGGER.error("Got failure code " + statusCode + " when handling route");
                response.setStatusCode(statusCode).end();
            }

            metric.markCode(statusCode);
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
            String platform = Strings.nullToEmpty(request.getParam("platform"));
            String systemVersion = Strings.nullToEmpty(request.getParam("systemVersion"));
            String resolution = Strings.nullToEmpty(request.getParam("resolution"));
            String appVersion = Strings.nullToEmpty(request.getParam("appVersion"));
            String phoneType = Strings.nullToEmpty(request.getParam("phoneType"));
            String[] widthAndHeight = resolution.split("\\*");
            Integer resolutionWidth = 0;
            Integer resolutionHeight = 0;
            if (widthAndHeight.length >= 2) {
                resolutionWidth = Integer.valueOf(widthAndHeight[0]);
                resolutionHeight = Integer.valueOf(widthAndHeight[1]);
            }

            // 设置请求域上下文变量
            RequestScope requestScope = new RequestScope();
            requestScope.httpClient = httpClient;
            requestScope.mongoClient = mongoClient;
            requestScope.userId = userId;
            requestScope.deviceId = deviceId;
            requestScope.platform = platform;
            requestScope.systemVersion = systemVersion;
            requestScope.resolutionWidth = resolutionWidth;
            requestScope.resolutionHeight = resolutionHeight;
            requestScope.appVersion = appVersion;
            requestScope.phoneType = phoneType;
            requestScope.errorCode = ErrorCode.SUCCESS;

            // 取inputContext
            InputContexts.get(requestScope, inputContext -> {

                // 取上次状态
                registry.get(requestScope, lastResult -> {

                    // 哈希分配
                    ProctorResult proctorResult = determineTestBucketMap(userId, deviceId, inputContext);

                    // 合并结果
                    combine(proctorResult, lastResult, userId, deviceId);

                    // 转换结果
                    JsonObject jsonResponse = toJson(proctorResult);

                    // 是否超时
                    if (response.ended()) return;

                    response.endHandler(v -> {

                        // 记录状态
                        registry.update(requestScope, proctorResult);

                        // 保存日志
                        //saveLog(userId, deviceId, jsonResponse);
                    });

                    // 发送应答
                    response.end(jsonResponse.encode());

                    // 记录总数
                    metric.markCode(requestScope.errorCode);
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

        router.get("/_simulate").handler(routingContext -> {
            HttpServerRequest request = routingContext.request();
            HttpServerResponse response = routingContext.response();
            response.setChunked(true);

            // 设置总样本数和检查点
            String sampleSizeStr = request.getParam("sampleSize");
            if (sampleSizeStr == null) sampleSizeStr = "1000000";
            int sampleSize = Integer.parseInt(sampleSizeStr);
            int checkPoint = 100000;

            // 设置前提条件
            Map<String, Object> inputContext = Maps.newHashMap();

            // 设置分布为二级树，统计进入每个实验的用户进入其它实验的情况
            Map<String, Map<String, AtomicInteger>> distribution = Maps.newHashMap();
            List<String> allBuckets = Lists.newArrayList();
            proctor.getArtifact().getTests().forEach((testId, testDef) -> {
                testDef.getBuckets().forEach(testBucket -> {
                    String distributionKey = testId + "/" + testBucket.getName();
                    allBuckets.add(distributionKey);
                });
            });
            allBuckets.forEach(distributionKey -> {
                Map<String, AtomicInteger> others = Maps.newHashMap();
                allBuckets.stream().filter(k -> !k.equals(distributionKey))
                        .forEach(k -> {
                            others.put(k, new AtomicInteger(0));
                        });
                others.put("total", new AtomicInteger(0));
                distribution.put(distributionKey, others);
            });

            // 开始模拟
            IntStream.range(1, sampleSize+1).forEach(i -> {
                // 生成随机用户
                String userId = String.valueOf(i);
                String deviceId = UUID.randomUUID().toString().replace("-", "");
                inputContext.put("userId", userId);
                inputContext.put("deviceId", deviceId);

                // 记录每次模拟迭代的输出
                ProctorResult proctorResult = determineTestBucketMap(userId, deviceId, inputContext);
                Map<String, TestBucket> buckets = proctorResult.getBuckets();
                List<String> all = Lists.newArrayList();
                buckets.forEach((testId, bucket) -> {
                    String distributionKey = testId + "/" + bucket.getName();
                    all.add(distributionKey);
                });
                all.forEach(distributionKey -> {
                    Map<String, AtomicInteger> subDist = distribution.get(distributionKey);
                    subDist.get("total").incrementAndGet();
                    all.stream().filter(k -> !k.equals(distributionKey))
                            .forEach(k -> {
                                subDist.get(k).incrementAndGet();
                            });
                });

                // 检查点打印输出
                if (i % checkPoint == 0) {
                    response.write("Sample size is " + i + ": the last userId is " + userId + ", deviceId is " + deviceId + "\n");
                    distribution.keySet().stream().sorted().forEach(key -> {
                        Map<String, AtomicInteger> subDist = distribution.get(key);
                        int total = subDist.get("total").intValue();
                        double totalRatio = total * 100.0 / i;
                        response.write("  Experiment bucket: " + key + "\t\t" + total + "\t(" + totalRatio + "%)\n");
                        subDist.keySet().stream().sorted().filter(k -> !k.equals("total"))
                                .forEach(k -> {
                                    int count = subDist.get(k).intValue();
                                    double countRatio = count * 100.0 / i;
                                    response.write("    Intersection: " + k + "\t\t" + count + "\t(" + countRatio + "%)\n");
                                });
                    });
                    response.write("\n");
                }
            });

            response.write("Done simulation\n");
            response.end();
        });
    }

    /**
     * 把 lastResult 合并到 proctorResult
     * @param proctorResult 当前实验结果，合并之后，会被改变
     * @param lastResult 上次实验结果，只读
     */
    private void combine(ProctorResult proctorResult, ProctorResult lastResult, String userId, String deviceId) {
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
                    TestBucket lastBucket = lastResult.getBuckets().get(testId);
                    TestBucket currBucket = proctorResult.getBuckets().put(testId, lastBucket);
                    if (currBucket != null) {
                        // 如果设备在白名单里，会进入此分支！
                        LOGGER.warn("A paused experiment `" + testId + "` is present: userId=" + userId +
                                " deviceId=" + deviceId + " currBucket=" + currBucket +
                                " lastBucket=" + lastBucket);
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
            MetricsClient.getDefMetricsValue(key).markCode(ErrorCode.SUCCESS2);
        }
        JsonObject json = new JsonObject();
        json.put("code", ErrorCode.SUCCESS2);  // 成功
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
