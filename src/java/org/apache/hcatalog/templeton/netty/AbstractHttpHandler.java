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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;

@ChannelPipelineCoverage("one")
public abstract class AbstractHttpHandler extends SimpleChannelUpstreamHandler {

    protected HttpRequest currentRequest;

    public static ChannelGroup allChannels = new DefaultChannelGroup("HttpServer");

    protected abstract void handleRequest(HttpRequest request, Channel userChannel)
        throws Exception;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception
    {
        Object message = e.getMessage();
        Channel userChannel = e.getChannel();
        boolean processMessage = false;
        if (message instanceof HttpChunk) {
            HttpChunk httpChunk = (HttpChunk) message;
            if (currentRequest == null)
                throw new IllegalStateException("No chunk start");
            ChannelBuffer channelBuffer = currentRequest.getContent();
            if (channelBuffer == null)
                throw new IllegalStateException("No chunk start");
            ChannelBuffer compositeBuffer
                = ChannelBuffers.wrappedBuffer(channelBuffer, httpChunk.getContent());
            currentRequest.setContent(compositeBuffer);
            processMessage = httpChunk.isLast();
        } else if (message instanceof HttpRequest){
            currentRequest = (HttpRequest) message;
            processMessage = ! currentRequest.isChunked();
        }

        if (processMessage) {
            handleRequest(currentRequest, userChannel);
        }
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception
    {
        allChannels.add(e.getChannel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception
    {
        e.getCause().printStackTrace();
        e.getChannel().close();
    }
}
