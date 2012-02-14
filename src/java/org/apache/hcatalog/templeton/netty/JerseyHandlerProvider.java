package org.apache.hcatalog.templeton.netty;

import com.sun.jersey.spi.container.ContainerProvider;
import com.sun.jersey.spi.container.WebApplication;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.container.ContainerException;

public class JerseyHandlerProvider
    implements ContainerProvider<JerseyHandler>
{
    public JerseyHandler createContainer(Class<JerseyHandler> type,
                                          ResourceConfig config,
                                          WebApplication application)
        throws ContainerException
    {
        if (type != JerseyHandler.class)
            return null;

        return new JerseyHandler(application, config);
    }
}
