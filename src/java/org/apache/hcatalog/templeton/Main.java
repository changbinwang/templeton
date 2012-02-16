/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hcatalog.templeton;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hcatalog.templeton.netty.HttpServerPipelineFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;

/**
 * The main executable that starts up and runs the Server.
 */
public class Main {
    private static final Log LOG = LogFactory.getLog(AppConfig.class);

    /**
     * More threads than we can handle, but an upper limit so the
     * server won't crash.
     */
    public static final int MAX_THREADS = 1024;

    public static final int DEFAULT_PORT = 8080;

    public static final String TEMPLETON_L4J = "templeton-log4j.properties";

    private static volatile AppConfig conf;

    /**
     * Retrieve the config singleton.
     */
    public static synchronized AppConfig getAppConfigInstance() {
        if (conf == null)
            LOG.error("Bug: configuration not yet loaded");
        return conf;
    }

    public Main(String[] args) {
        init(args);
    }

    public void init(String[] args) {
        conf = loadConfig(args);
        LOG.debug("Loaded conf " + conf);
    }

    public AppConfig loadConfig(String[] args) {
        AppConfig cf = new AppConfig();
        try {
            GenericOptionsParser parser = new GenericOptionsParser(cf, args);
            if (parser.getRemainingArgs().length > 0)
                usage();
        } catch (IOException e) {
            LOG.error("Unable to parse options: " + e);
            usage();
        }

        return cf;
    }

    public void usage() {
        System.err.println("usage: templeton [-Dtempleton.port=N] [-D...]");
        System.exit(1);
    }

    public void run() {
        try {
            ZooKeeperCleanup.startInstance(conf);
        } catch (IOException e) {
            LOG.error("ZookeeperCleanup failed to start: " + e.getMessage());
        }

        runServer(conf.getInt(AppConfig.PORT, DEFAULT_PORT));
    }

    public void runServer(int port) {
        ThreadPoolExecutor boss = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ThreadPoolExecutor worker = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        boss.setMaximumPoolSize(MAX_THREADS);
        worker.setMaximumPoolSize(MAX_THREADS);

        OioServerSocketChannelFactory factory
            = new OioServerSocketChannelFactory(boss, worker);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        String className = Server.class.getName();
        bootstrap.setPipelineFactory(new HttpServerPipelineFactory(className));
        LOG.info("Templeton listening on port " + port);
        try {
            bootstrap.bind(new InetSocketAddress(port));
        } catch (ChannelException e) {
            System.err.println("templeton: Server failed to start: " + e.getMessage());
            LOG.fatal("Server failed to start: " + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        Main templeton = new Main(args);
        templeton.run();
    }
}
