package com.diamis.horus.httptojms;

import java.io.IOException;

import org.glassfish.grizzly.http.server.HttpServer;

import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.DefaultResourceConfig;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IllegalArgumentException, NullPointerException, IOException
    {
        DefaultResourceConfig resourceConfig = new DefaultResourceConfig(HorusHttpEndpoint.class);
        // The following line is to enable GZIP when client accepts it
        resourceConfig.getContainerResponseFilters().add(new GZIPContentEncodingFilter());
        HttpServer server = GrizzlyServerFactory.createHttpServer("http://0.0.0.0:8080" , resourceConfig);
        try {
            System.out.println("Press any key to stop the service...");
            System.in.read();
        } finally {
            server.stop();
        }
    }
}
