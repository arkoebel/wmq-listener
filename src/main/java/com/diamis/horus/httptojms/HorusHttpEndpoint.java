package com.diamis.horus.httptojms;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import javax.annotation.security.PermitAll;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.BodyPart;

import com.diamis.horus.HorusException;
import com.diamis.horus.HorusUtils;

@Path("/horustojms")
public class HorusHttpEndpoint {

	private Map<String,String> convertHeaders(HttpHeaders headers){
		Map<String,String> result = new HashMap<String,String>();
		for (Map.Entry<String,List<String>> entry : headers.getRequestHeaders().entrySet()) {
			try {
				result.put(entry.getKey(),URLDecoder.decode(entry.getValue().get(0),"UTF-8"));
			} catch (UnsupportedEncodingException e) {
				//nothing
			}
		}
		return result;
	}

	@Context
	private ResourceConfig ctx;

	@POST
	@Consumes("application/xml")
	@Produces("application/json")
	public String setMessageXml(String body, @HeaderParam("X-Business-Id") String business_id,
			@HeaderParam("X-B3-TraceId") String traceid, @HeaderParam("X-B3-SpanId") String spanid,
			@HeaderParam("X-B3-ParentSpanId") String parentspanid, @HeaderParam("X-B3-Sampled") String sampled,
			@Context HttpHeaders headers) {
		String jmsQueue = ctx.getProperty("jmsQueue").toString();
		HorusUtils.logJson("INFO", business_id, jmsQueue, "Message XML : " + body);
		long start = 0;
		long stop = 0;
		try {
			start = System.nanoTime();
			Map<String,Map<String,String>> strippedHeaders = JMSProducer.stripHeaders(convertHeaders(headers));
			JMSProducer.sendMessage(body, business_id, traceid, spanid, parentspanid, sampled,strippedHeaders);
			stop = System.nanoTime();
			String returnMessage = "{\"status\": \"OK\",\"time\":\"" + ((stop - start) / 1000000) + "\"}";
			HorusUtils.logJson("INFO", business_id, jmsQueue, "Return OK in " + ((stop - start) / 1000000));
			return returnMessage;
		} catch (HorusException e) {
			HorusUtils.logJson("ERROR", business_id, jmsQueue, "Return KO: " + e.getMessage());
			HorusUtils.logJson("DEBUG", business_id, jmsQueue, e.getStackTrace().toString());
			return "{\"status\": \"KO\",\"time\":\"" + ((stop - start) / 1000000) + ",\"message\": \""
					+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
		}

	}

	@POST
	@Consumes("application/json")
	@Produces("application/json")
	public String setMessageJson(String body, @HeaderParam("X-Business-Id") String business_id,
			@HeaderParam("X-B3-TraceId") String traceid, @HeaderParam("X-B3-SpanId") String spanid,
			@HeaderParam("X-B3-ParentSpanId") String parentspanid, @HeaderParam("X-B3-Sampled") String sampled,
			@Context HttpHeaders headers) {
		String jmsQueue = ctx.getProperty("jmsQueue").toString();
		JsonParser json = new JsonParser();
		HorusUtils.logJson("INFO", business_id, jmsQueue, "Got JSON message");
		HorusUtils.logJson("INFO", business_id, jmsQueue, "Incoming JSON Message : " + body);
		
		JsonElement elt = json.parse(body.trim());
		JsonObject obj = elt.getAsJsonObject();
		String bodyjson;
		if (obj.get("payload") == null) {
			bodyjson = body;
		} else {
			bodyjson = obj.get("payload").getAsString();
		}
		HorusUtils.logJson("DEBUG", business_id, jmsQueue, "Decoded JSON Message : " + bodyjson);
		long start = 0;
		long stop = 0;
		try {
			start = System.nanoTime();
			Map<String,Map<String,String>> strippedHeaders = JMSProducer.stripHeaders(convertHeaders(headers));
			JMSProducer.sendMessage(bodyjson, business_id, traceid, spanid, parentspanid, sampled,strippedHeaders);
			stop = System.nanoTime();
			HorusUtils.logJson("INFO", business_id, jmsQueue, "Return OK in " + ((stop - start) / 1000000));
			return "{\"status\": \"OK\",\"time\":\"" + ((stop - start) / 1000000) + "\"}";
		} catch (HorusException e) {
			HorusUtils.logJson("INFO", business_id, jmsQueue, "Return KO: " + e.getMessage());
			HorusUtils.logJson("DEBUG", business_id, jmsQueue, e.getStackTrace().toString());
			stop = System.nanoTime();
			return "{\"status\": \"KO\",\"time\":\"" + ((stop - start) / 1000000) + "\",\"message\": \""
					+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
		}

	}

	@POST
	@Consumes("text/plain")
	@Produces("application/json")
	public String setMessageText(String body, @HeaderParam("X-Business-Id") String business_id,
			@HeaderParam("X-B3-TraceId") String traceid, @HeaderParam("X-B3-SpanId") String spanid,
			@HeaderParam("X-B3-ParentSpanId") String parentspanid, @HeaderParam("X-B3-Sampled") String sampled,@Context HttpHeaders headers) {
		String jmsQueue = ctx.getProperty("jmsQueue").toString();
		HorusUtils.logJson("INFO", business_id, jmsQueue, "Got Text Message");
		HorusUtils.logJson("INFO", business_id, jmsQueue, "Incoming Text Message : " + body);
		String newbody = StringUtils.replace(body, "\\0d\\0a", "\r\n", -1);
		HorusUtils.logJson("DEBUG", business_id, jmsQueue,
				"String with control chars : " + StringEscapeUtils.escapeJava(newbody));
		long start = 0;
		long stop = 0;
		try {
			start = System.nanoTime();
			Map<String,Map<String,String>> strippedHeaders = JMSProducer.stripHeaders(convertHeaders(headers));
			
			JMSProducer.sendMessage(newbody, business_id, traceid, spanid, parentspanid, sampled,strippedHeaders);
			stop = System.nanoTime();
			HorusUtils.logJson("INFO", business_id, jmsQueue, "Return OK in " + ((stop - start) / 1000000));
			return "{\"status\": \"OK\",\"time\":\"" + ((stop - start) / 1000000) + "\"}";
		} catch (HorusException e) {
			HorusUtils.logJson("INFO", business_id, jmsQueue, "Return KO: " + e.getMessage());
			HorusUtils.logJson("DEBUG", business_id, jmsQueue, e.getStackTrace().toString());
			stop = System.nanoTime();
			return "{\"status\": \"KO\",\"time\":\"" + ((stop - start) / 1000000) + "\",\"message\": \""
					+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
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
		return Response.ok() // 200
				.header("Access-Control-Allow-Origin", "*").header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				.header("Access-Control-Allow-Headers",
						"Access-Control-Allow-Methods, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Origin,Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers")
				.header("Access-Control-Request-Headers", "Access-Control-Allow-Origin, Content-Type").build();
	}

	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces("application/json")
	public String setMessageMultipart(FormDataMultiPart files, 
										@HeaderParam("X-Business-Id") String business_id,
										@HeaderParam("X-B3-TraceId") String traceid, @HeaderParam("X-B3-SpanId") String spanid,
										@HeaderParam("X-B3-ParentSpanId") String parentspanid, @HeaderParam("X-B3-Sampled") String sampled,
										@Context HttpHeaders headers){
		String jmsQueue = ctx.getProperty("jmsQueue").toString();
		HorusUtils.logJson("INFO", business_id, jmsQueue, "Message MultiPart");
		Map<String,Map<String,String>> strippedHeaders = JMSProducer.stripHeaders(convertHeaders(headers));
		int i=0;
		long start=System.nanoTime();
		long stop=0;
		
		for(BodyPart filePart: files.getBodyParts()){
			i++;
			String bodyPart = filePart.getEntityAs(String.class).toString();

			HorusUtils.logJson("INFO", business_id, jmsQueue, "Extracted body part " + i + "(Type=" + filePart.getMediaType() +  ") :" + bodyPart);

			try {
				JMSProducer.sendMessage(bodyPart, business_id, traceid, spanid, parentspanid, sampled,strippedHeaders);
			} catch (HorusException e) {
				HorusUtils.logJson("ERROR", business_id, jmsQueue, "Return KO for part " + i + ": " + e.getMessage());
				HorusUtils.logJson("DEBUG", business_id, jmsQueue, e.getStackTrace().toString());
				stop=System.nanoTime();
				return "{\"status\": \"KO\",\"time\":\"" + ((stop - start) / 1000000) + ",\"message\": \"Error on part #" +i +"-"
						+ e.getMessage().replaceAll("\"", "\\\"") + "\"}";
			}

		}
		stop=System.nanoTime();
		String returnMessage = "{\"status\": \"OK\",\"time\":\"" + ((stop - start) / 1000000) + "\"}";
			HorusUtils.logJson("INFO", business_id, jmsQueue, "Return OK in " + ((stop - start) / 1000000));
		return returnMessage;
	}
}
