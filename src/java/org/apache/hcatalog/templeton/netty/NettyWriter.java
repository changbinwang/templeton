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

import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

public final class NettyWriter implements ContainerResponseWriter {
     private final Channel channel;
     private HttpResponse response;

     public NettyWriter(Channel channel) {
         this.channel = channel;
     }

     public OutputStream writeStatusAndHeaders(long contentLength,
                                               ContainerResponse cResponse)
         throws IOException
     {
         response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                            HttpResponseStatus.valueOf(cResponse.
                                            getStatus()));

         for (Map.Entry<String, List<Object>> e
                  : cResponse.getHttpHeaders().entrySet())
         {
             ArrayList<String> values = new ArrayList<String>();
             for (Object v : e.getValue())
                 values.add(ContainerResponse.getHeaderValue(v));
             response.setHeader(e.getKey(), values);
         }

         ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
         response.setContent(buffer);
         return new ChannelBufferOutputStream(buffer);
     }

     public void finish() throws IOException
     {
         channel.write(response).addListener(ChannelFutureListener.CLOSE);
     }
 }
