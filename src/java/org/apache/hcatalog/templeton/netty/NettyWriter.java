package org.apache.hcatalog.templeton.netty;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.io.OutputStream;
import org.jboss.netty.handler.codec.http.*; 
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

public final class NettyWriter implements ContainerResponseWriter
 {
     private final Channel channel;
     private HttpResponse response;
     
     public NettyWriter(Channel channel)
     {
         this.channel = channel;
     }

     public OutputStream writeStatusAndHeaders(long contentLength,
                                               ContainerResponse cResponse)
         throws IOException
     {
        
         response = new DefaultHttpResponse(HttpVersion.HTTP_1_1,
                                            HttpResponseStatus.valueOf(cResponse.
                                            getStatus()));
         
         for (Map.Entry<String, List<Object>> e :
                  cResponse.getHttpHeaders().entrySet())
         {
             List<String> values = new ArrayList<String>();
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