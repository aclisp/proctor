package com.indeed.proctor.common.server;

public class Config {

    final static String MONGODB_CONNECTION_STRING = "mongodb://abman:w80IgG8ebQq@221.228.107.70:10005,183.36.121.130:10006,61.140.10.115:10003/abtest";
    //final static String MONGODB_CONNECTION_STRING = "mongodb://172.27.142.6:27017/abtest";

    public final static String EXPERIMENT_SOURCE = "https://abtest.yy.com/api/testList";
    //public final static String EXPERIMENT_SOURCE = "http://127.0.0.1:10100/proctor/adminModel.json";

    final static String HIIDO_SERVICE_URL = "http://szhiidocosevice.yy.com/service/read";

    final static String FALLBACK_TEST_MATRIX = "/data/yy/vxlog/abtesting/test-matrix.json";

    final static int SERVER_INSTANCE_NUMBER = 1;
}
