package com.diamis.horus;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class HorusUtils {

	public static String BLUE_BOX = "BLUE";
	public static String PINK_BOX = "PINK";

	public static void logJson(String priority, String business_id,String queue, String message ){
		HorusUtils.logJson(HorusUtils.BLUE_BOX, priority, business_id, queue, message);
	}

	public static void logJson(String box, String priority, String business_id,String queue, String message ){

		Logger logger = Logger.getLogger(HorusUtils.class);
		Map<String,String> map = new HashMap<String,String>();
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); 
		df.setTimeZone(tz);
		String event = df.format(new Date());

		map.put("program",box);
		map.put("log_level",priority);
		map.put("timestamp",event);
		map.put("business_id",business_id);
		map.put("file",queue);
		map.put("message",message);
		Gson gson = new GsonBuilder().create();
		Level prio = Level.toLevel(priority);
		logger.log(prio,gson.toJson(map));
	}
}