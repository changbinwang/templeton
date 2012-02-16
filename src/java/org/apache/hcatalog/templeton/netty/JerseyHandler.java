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

import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.header.InBoundHeaders;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.WebApplication;
import java.net.URI;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;

@ChannelPipelineCoverage("one")
public class JerseyHandler extends AbstractHttpHandler {

    private WebApplication application;

    public JerseyHandler(WebApplication application,
                         ResourceConfig resourceConfig)
    {
        this.application = application;
    }

    protected void handleRequest(HttpRequest request, Channel userChannel)
        throws Exception
    {
        String base = getBaseUri(request);
        final URI baseUri = new URI(base);
        final URI requestUri = new URI(base.substring(0, base.length() - 1)
                                       + request.getUri());

        final ContainerRequest cRequest = new ContainerRequest(
            application,
            request.getMethod().getName(),
            baseUri,
            requestUri,
            getHeaders(request),
            new ChannelBufferInputStream(request.getContent())
            );

        application.handleRequest(cRequest, new NettyWriter(userChannel));
    }


    private String getBaseUri(HttpRequest request) {
        return "http://" + request.getHeader(HttpHeaders.Names.HOST) + "/";
    }

    private InBoundHeaders getHeaders(HttpRequest request) {
        InBoundHeaders headers = new InBoundHeaders();
        for (String name : request.getHeaderNames()){
            headers.put(name, request.getHeaders(name));
        }
        return headers;
    }
}
