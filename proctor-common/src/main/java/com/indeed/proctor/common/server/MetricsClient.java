package com.indeed.proctor.common.server;

import com.duowan.sysop.hawk.metrics.client2.type.DefMetricsValue;
import com.duowan.sysop.hawk.metrics.client2.type.DefaultModel;
import com.duowan.sysop.hawk.metrics.client2.type.MetricsModelFactory;
import com.duowan.sysop.hawk.metrics.client2.type.TimeScale;
import io.vertx.ext.web.RoutingContext;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class MetricsClient {
    private static final Logger LOGGER = Logger.getLogger(MetricsClient.class);

    private String appName;
    private String serviceName;
    private String serviceVersion;
    private MetricsModelFactory metricsFactory;
    private DefaultModel metrics;

    private MetricsClient(String appName, String serviceName, String serviceVersion) {
        this.appName = appName;
        this.serviceName = serviceName;
        this.serviceVersion = serviceVersion;

        initMetricsFactory();
    }

    private void initMetricsFactory() {
        long[] scales = {5, 10, 20, 30, 50, 100, 200, 300, 500, 800, 1000, 1500, 2000, 5000, 10000};

        MetricsModelFactory.Builder builder = new MetricsModelFactory.Builder(
                appName,                    //应用名
                serviceName,                //服务名
                serviceVersion,             //服务版本
                new TimeScale(scales, TimeUnit.MILLISECONDS),                     //时延分布
                false,           //isFailureCode, false:第6个参数是成功码,true: 第6个参数是失败码
                ErrorCode.SUCCESS2,
                ErrorCode.SUCCESS,
                ErrorCode.SUCCESS_HTTP_NO_CONTENT);             //状态码,表示成功或失败的代码,支持多个,要么int,要么字符串

        builder.notSkipInitialPeriod();                 //指定不丢弃第1个上报周期的数据,建议设置
        builder.period(MetricsModelFactory.Period._1Min);        //设置上报周期为1分钟,还支持5分钟,(默认是1分钟)

        //创建工厂实例，一个服务使用同一个即可
        metricsFactory = builder.build();

        //缺省模型实例,可为单例,后面都用这个报吧
        metrics = metricsFactory.defaultModel();
    }

    public DefaultModel defaultModel() {
        return metrics;
    }

    // 封装为单例
    private static MetricsClient instance;

    public static MetricsClient get() {
        return instance;
    }

    /**
     * Setup must be called only once
     */
    public static void setup() {
        String appName = "abtesting-apiserver";
        String serviceName = "default";
        String serviceVersion = "1.0.0";

        instance = new MetricsClient(appName, serviceName, serviceVersion);

        LOGGER.info("create metrics instance with appName=" + appName +
                ", serviceName=" + serviceName +
                ", serviceVersion=" + serviceVersion);
    }

    /**
     * Helpers
     */
    public static DefMetricsValue getDefMetricsValue(RoutingContext routingContext) {
        String key = "route" + routingContext.currentRoute().getPath();
        return getDefMetricsValue(key);
    }

    public static DefMetricsValue getDefMetricsValue(String key) {
        DefMetricsValue v = MetricsClient.get().defaultModel().get(key);
        if (v == null || !v.isValid()) {
            LOGGER.error("MetricsClient.get().defaultModel().get(" + key + ") is not valid");
        }
        return v;
    }
}
