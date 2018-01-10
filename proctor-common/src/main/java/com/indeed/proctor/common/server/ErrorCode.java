package com.indeed.proctor.common.server;

public class ErrorCode {
    final static int SUCCESS = 200;
    final static int SUCCESS_HTTP_NO_CONTENT = 204;
    final static int SUCCESS2 = 0;
    final static int NOT_FOUND = 404;
    final static int INTERNAL_SERVER_ERROR = 500;
    final static int INTERNAL_SERVER_ERROR2 = -1;
    final static int SERVER_PROCESSING_TIMEOUT = 524;
    final static int HIIDO_SERVICE_RESPONSE_CODE_IS_NOT_200 = 601;
    final static int HIIDO_SERVICE_RESPONSE_PARSE_ERROR = 602;
    final static int HIIDO_SERVICE_UNAVAILABLE = 603;
    final static int MONGODB_READ_ERROR = 651;
    final static int MONGODB_WRITE_ERROR = 652;
}
