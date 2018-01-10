package com.indeed.proctor.common.server;

import com.duowan.sysop.hawk.metrics.client2.type.DefMetricsValue;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.mongo.MongoClient;

public class RequestScope {
    HttpClient httpClient;
    MongoClient mongoClient;
    String userId;
    String deviceId;
    int errorCode;
}
