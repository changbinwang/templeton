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
package org.apache.hcatalog.templeton.netty;

import com.sun.jersey.api.container.ContainerFactory;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import java.util.HashMap;
import java.util.ArrayList;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

/**
 * Pipeline to connect netty with Jersey.
 */
public class HttpServerPipelineFactory implements ChannelPipelineFactory {
    private JerseyHandler jerseyHandler;
    private String[] providers;

    public HttpServerPipelineFactory(String... providers) {
        this.providers = providers;
        this.jerseyHandler = getJerseyHandler();
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("decoder", new HttpRequestDecoder());
        p.addLast("encoder", new HttpResponseEncoder());
        p.addLast("jerseyHandler", jerseyHandler);
        return p;
    }

    private JerseyHandler getJerseyHandler() {
        HashMap<String, Object> props = new HashMap<String, Object>();
        props.put(ClassNamesResourceConfig.PROPERTY_CLASSNAMES, providers);
        props.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
        // props.put(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS,
        //          org.apache.hadoop.hdfs.web.AuthFilter.class.getName());

        ResourceConfig rcf = new ClassNamesResourceConfig(props);
        return ContainerFactory.createContainer(JerseyHandler.class, rcf);
    }
}
