package com.diamis.horus.httptojms;

import javax.jms.JMSException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.annotation.security.PermitAll;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Path("/horustojms")
public class HorusHttpEndpoint {
	
		Logger logger = Logger.getLogger(HorusHttpEndpoint.class);
	
	    @POST
	    @Consumes("application/xml")
	    @Produces("application/json")
	    public String setMessageXml(String body){
	    	logger.debug("Message XML : " + body);
	    	long start=0;
	    	long stop=0;
	        try {
	        	start = System.nanoTime();
				JMSProducer.sendMessage(body);
				stop = System.nanoTime();
				String returnMessage = "{\"status\": \"OK\",\"time\":\""+((stop-start)/1000000)+"\"}";
				logger.info("Return OK in " + ((stop-start)/1000000));
				return returnMessage;
			} catch (JMSException e) {
				logger.info("Return KO: " + e.getMessage());
				logger.debug(e.getStackTrace());
				return "{\"status\": \"KO\",\"time\":\""+((stop-start)/1000000)+",\"message\": \""+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
			}
	    	 
	    }
	    
	    @POST
	    @Consumes("application/json")
	    @Produces("application/json")
	    public String setMessageJson(String body){
	    	JsonParser json = new JsonParser();
	    	logger.info("Got JSON message");
	    	logger.debug("Incoming JSON Message : " + body);
	    	JsonElement elt = json.parse(body.trim());
	    	JsonObject obj = elt.getAsJsonObject();
	    	String bodyjson;
	    	if (obj.get("payload")==null) {
	    		bodyjson = body;
	    	}else {
	    		bodyjson = obj.get("payload").getAsString();
	    	}
	    	logger.debug("Decoded JSON Message : " + bodyjson);
	    	long start=0;
	    	long stop=0;
	        try {
	        	start = System.nanoTime();
				JMSProducer.sendMessage(bodyjson);
				stop = System.nanoTime();
				logger.info("Return OK in "+ ((stop-start)/1000000));
				return "{\"status\": \"OK\",\"time\":\""+((stop-start)/1000000)+"\"}";
			} catch (JMSException e) {
				logger.info("Return KO: " + e.getMessage());
				logger.debug(e.getStackTrace());
				stop = System.nanoTime();
				return "{\"status\": \"KO\",\"time\":\""+((stop-start)/1000000)+"\",\"message\": \""+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
			}
	    	 
	    }
	    
	    @POST
	    @Consumes("text/plain")
	    @Produces("application/json")
	    public String setMessageText(String body){
	    	logger.info("Got Text Message");
	    	logger.debug("Incoming Text Message : " + body);
	    	String newbody = StringUtils.replace(body, "\\0d\\0a", "\r\n",-1);
	    	logger.debug("String with control chars : " + StringEscapeUtils.escapeJava(newbody));
	    	long start=0;
	    	long stop=0;
	        try {
	        	start = System.nanoTime();
				JMSProducer.sendMessage(newbody);
				stop = System.nanoTime();
				logger.info("Return OK in " + ((stop-start)/1000000));
				return "{\"status\": \"OK\",\"time\":\""+((stop-start)/1000000)+"\"}";
			} catch (JMSException e) {
				logger.info("Return KO: " + e.getMessage());
				logger.debug(e.getStackTrace());
				stop = System.nanoTime();
				return "{\"status\": \"KO\",\"time\":\""+((stop-start)/1000000)+"\",\"message\": \""+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
			}
	    	 
	    }
	    
	    @GET
	    @Produces("application/json")
	    public String getStatus() {
	    	return "{\"status\": \"OK\"}";
	    }
	    
	    @OPTIONS
	    @PermitAll
	    public Response options() {
	        return Response.ok() //200
	                .header("Access-Control-Allow-Origin", "*")             
	                .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	                .header("Access-Control-Allow-Headers", "Access-Control-Allow-Methods, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers")
	                .header("Access-Control-Request-Headers", "Access-Control-Allow-Origin, Content-Type")
	                .build();
	    }

}
