package com.indeed.proctor.common.server;

import com.google.common.collect.Maps;
import com.indeed.proctor.common.ProctorResult;
import com.indeed.proctor.common.model.TestBucket;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.Date;
import java.util.Map;


public class MongoRegistry extends Registry {

    private static final Logger LOGGER = Logger.getLogger(MongoRegistry.class);
    private final static String collection = "registry";

    public MongoRegistry(@Nonnull Vertx vertx, @Nonnull Context context, @Nonnull MongoClient mongoClient) {
        super(vertx, context, mongoClient);
    }

    @Override
    public void setupPeriodic() {

    }

    @Override
    public void update(@Nonnull String userId, @Nonnull String deviceId, @Nonnull ProctorResult proctorResult) {
        Map<String, String> data = Maps.newHashMap();
        proctorResult.getBuckets().forEach((testId, testBucket) -> {
            // 为了减小存储容量，只关心bucketName
            data.put(testId, testBucket.getName());
        });
        JsonObject document = new JsonObject()
                .put("_id", deviceId)
                .put("data", data)
                .put("timestamp", System.currentTimeMillis());
        mongoClient.save(collection, document, res -> {
            if (res.failed()) {
                LOGGER.error("Unable save to collection " + collection + ": " + res.cause().toString());
            }
        });
    }

    @Override
    public void get(@Nonnull String userId, @Nonnull String deviceId, @Nonnull Handler<ProctorResult> resultHandler) {
        JsonObject query = new JsonObject().put("_id", deviceId);
        JsonObject fields = new JsonObject().put("data", 1);
        mongoClient.findOne(collection, query, fields, res -> {
            if (res.succeeded()) {
                JsonObject json = res.result();
                if (json == null) {
                    resultHandler.handle(null);
                    return;
                }
                Map<String, Object> data = json.getJsonObject("data").getMap();
                Map<String, TestBucket> buckets = Maps.newHashMap();
                data.forEach((testId, bucketName) -> {
                    TestBucket bucket = new TestBucket();
                    bucket.setName((String) bucketName);
                    buckets.put(testId, bucket);
                });
                ProctorResult proctorResult = new ProctorResult();
                proctorResult.setBuckets(buckets);
                resultHandler.handle(proctorResult);
            } else {
                LOGGER.error("Unable get from collection " + collection + ": " + res.cause().toString());
            }
        });
    }
}
