package com.diamis.horus.httptojms;

import javax.jms.JMSException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Path("/horustojms")
public class HorusHttpEndpoint {
	
		Log logger = LogFactory.getLog(HorusHttpEndpoint.class);
	
	    @POST
	    @Consumes("application/xml")
	    @Produces("application/json")
	    public String setMessage(String body){
	    	logger.info("Message XML : " + body);
	    	long start=0;
	    	long stop=0;
	        try {
	        	start = System.nanoTime();
				JMSProducer.sendMessage(body);
				stop = System.nanoTime();
				return "{\"status\": \"OK\",\"time\":\""+((stop-start)/1000000)+"\"}";
			} catch (JMSException e) {
				return "{\"status\": \"KO\",\"time\":\""+((stop-start)/1000000)+",\"message\": \""+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
			}
	    	 
	    }
	    
	    @POST
	    @Consumes("application/json")
	    @Produces("application/json")
	    public String setMessageJson(String body){
	    	JsonParser json = new JsonParser();
	    	logger.info("Message JSON : " + body);
	    	JsonElement elt = json.parse(body.trim());
	    	JsonObject obj = elt.getAsJsonObject();
	    	String bodyxml;
	    	if (obj.get("payload")==null) {
	    		bodyxml = body;
	    	}else {
	    		bodyxml = obj.get("payload").getAsString();
	    	}
	    	logger.info("Decoded JSON Message : " + bodyxml);
	    	long start=0;
	    	long stop=0;
	        try {
	        	start = System.nanoTime();
				JMSProducer.sendMessage(bodyxml);
				stop = System.nanoTime();
				return "{\"status\": \"OK\",\"time\":\""+((stop-start)/1000000)+"\"}";
			} catch (JMSException e) {
				stop = System.nanoTime();
				return "{\"status\": \"KO\",\"time\":\""+((stop-start)/1000000)+"\",\"message\": \""+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
			}
	    	 
	    }
	    
	    @POST
	    @Consumes("text/plain")
	    @Produces("application/json")
	    public String setMessageText(String body){
	    	logger.info("Message Text : " + body);
	    	long start=0;
	    	long stop=0;
	        try {
	        	start = System.nanoTime();
				JMSProducer.sendMessage(body);
				stop = System.nanoTime();
				return "{\"status\": \"OK\",\"time\":\""+((stop-start)/1000000)+"\"}";
			} catch (JMSException e) {
				stop = System.nanoTime();
				return "{\"status\": \"KO\",\"time\":\""+((stop-start)/1000000)+"\",\"message\": \""+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
			}
	    	 
	    }
	    
	    @GET
	    @Produces("application/json")
	    public String getStatus() {
	    	return "{\"status\": \"OK\"}";
	    }

}
