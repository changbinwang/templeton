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

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hcatalog.templeton.netty.HttpServerPipelineFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;

/**
 * The main executable that starts up and runs the Server.
 */
public class Main {
    /**
     * More threads than we can handle, but an upper limit so the
     * server won't crash.
     */
    public static final int MAX_THREADS = 1024;

    public static final int DEFAULT_PORT = 8080;

    public static void main(String[] args) {
        ThreadPoolExecutor boss = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ThreadPoolExecutor worker = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        boss.setMaximumPoolSize(MAX_THREADS);
        worker.setMaximumPoolSize(MAX_THREADS);

        OioServerSocketChannelFactory factory
            = new OioServerSocketChannelFactory(boss, worker);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(new HttpServerPipelineFactory());
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + e);
                System.exit(1);
            }
        }

        System.out.println("Templeton listening on port:"+port);
        bootstrap.bind(new InetSocketAddress(port));
    }
}
