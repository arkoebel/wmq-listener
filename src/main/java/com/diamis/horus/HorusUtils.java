package com.diamis.horus;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.TimeZone;

import java.util.Date;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//   Logger;
import org.apache.logging.log4j.Level;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class HorusUtils {
	public static String BLUE_BOX = "BLUE";
	public static String PINK_BOX = "PINK";

	public static void logJson(String priority, String business_id, String queue, String message) {
		HorusUtils.logJson(HorusUtils.BLUE_BOX, priority, business_id, queue, message);
	}

	public static void logJson(String box, String priority, String business_id, String queue, String message) {

		Logger logger = LogManager.getLogger(HorusUtils.class);
		Map<String, String> map = new HashMap<String, String>();
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(tz);
		String event = df.format(new Date());

		map.put("program", box);
		map.put("log_level", priority);
		map.put("timestamp", event);
		map.put("business_id", business_id);
		map.put("file", queue);
		map.put("message", message);
		Gson gson = new GsonBuilder().create();
		Level prio = Level.toLevel(priority);
		logger.log(prio, gson.toJson(map));
	}

	public static List<String> splitDataPDU(String input, int length, String business_id, String jmsQueue) {
		List<String> messages = new ArrayList<String>();
		if (input.length()<length){
			messages.add(input);
			return messages;
		}
		Pattern pattern = Pattern.compile("<(.*):DataPDU ");
		Matcher matcher = pattern.matcher(input);
		boolean matchFound = matcher.find();
		String namespace = "";
		if (matchFound) {
			namespace = matcher.group(1) + ":";
		}
		pattern = Pattern.compile("<" + namespace + "Body>(.*)</" + namespace + "Body>");
		matcher = pattern.matcher(input);
		matchFound = matcher.find();
		if (!matchFound) {
			HorusUtils.logJson("ERROR", business_id, jmsQueue, "Message isn't DataPDU Format");
			messages.add(input);
		} else {
			String pdubody =  "<File>" + matcher.group(1)  + "</File>";
			String pduenv = input.replaceFirst("<" + namespace + "Body>(.*)</" + namespace + "Body>", "");
			messages.add(0, pduenv);
			int max = pdubody.length() / length;
			for (int i = 0; i <= max; i++)
				if (i < max)
					messages.add(i + 1, pdubody.substring(i * length, (i + 1) * length ));
				else
					messages.add(i + 1, pdubody.substring(i * length));

		}
		return messages;

	}
}