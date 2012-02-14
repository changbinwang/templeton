package org.apache.hcatalog.templeton.netty;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;

import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.header.InBoundHeaders;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.spi.container.ContainerRequest;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import java.net.URI;

@ChannelPipelineCoverage("one")
public class JerseyHandler extends AbstractHttpHandler {

    private WebApplication application;
   
	public JerseyHandler(WebApplication application,
                          ResourceConfig resourceConfig)
    {
		this.application = application;
	}
	
	protected void handleRequest(HttpRequest request, Channel userChannel) throws Exception {
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
	

    private String getBaseUri(HttpRequest request){    
        return "http://" + request.getHeader(HttpHeaders.Names.HOST) + "/";
    }
	
	private InBoundHeaders getHeaders(HttpRequest request)
    {
        InBoundHeaders headers = new InBoundHeaders();
        for (String name : request.getHeaderNames()){
            headers.put(name, request.getHeaders(name));
        }
        return headers;
    }
	
}
