package com.indeed.proctor.common.server;

import com.indeed.proctor.common.RemoteProctorLoader;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.util.Locale;
import java.util.Timer;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(final String[] args) {
        // Vertx require this
        Locale.setDefault(new Locale("en"));
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");

        /*
        LOGGER.info("setup timer for remoteProctorLoader");
        Timer timer = new Timer();
        try {
            RemoteProctorLoader remoteProctorLoader = RemoteProctorLoader.createInstance();
            timer.scheduleAtFixedRate(remoteProctorLoader, 0, 300*1000);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        */

        //TODO int instances = Runtime.getRuntime().availableProcessors();
        int instances = Config.SERVER_INSTANCE_NUMBER;
        LOGGER.info("setup vertx with " + instances + " instances");
        Vertx vertx = Vertx.vertx();
        DeploymentOptions options = new DeploymentOptions().setInstances(instances);
        vertx.deployVerticle(DefaultServer.class.getName(), options);
    }
}
