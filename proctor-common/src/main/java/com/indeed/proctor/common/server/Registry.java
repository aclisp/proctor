package com.indeed.proctor.common.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.indeed.proctor.common.ProctorResult;
import com.indeed.proctor.common.Serializers;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.mongo.MongoClient;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public abstract class Registry {

    @Nonnull
    protected Vertx vertx;
    @Nonnull
    protected Context context;
    @Nonnull
    protected MongoClient mongoClient;
    @Nonnull
    protected final ObjectMapper objectMapper = Serializers.lenient();

    public Registry(@Nonnull Vertx vertx, @Nonnull Context context, @Nonnull MongoClient mongoClient) {
        this.vertx = vertx;
        this.context = context;
        this.mongoClient = mongoClient;
    }

    abstract public void setupPeriodic();

    abstract public void update(@Nonnull String userId, @Nonnull String deviceId, @Nonnull ProctorResult proctorResult);

    abstract public void get(@Nonnull String userId, @Nonnull String deviceId, @Nonnull Handler<ProctorResult> resultHandler);
}
