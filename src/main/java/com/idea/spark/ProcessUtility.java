package com.idea.spark;

import java.util.HashMap;
import java.util.regex.Pattern;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class ProcessUtility {

	public static final Pattern SPACE = Pattern.compile(" ");
	public static MongoClient mongo = null;
	public static DB db = null;
	public static DBCollection table = null;
	public static DBCollection newLightsTable = null;
	public static HashMap<String, Lighting> lightsMap = new HashMap<String, Lighting>();
	public static HashMap<String, String> thermostatLocator = new HashMap();
	public static WaterHeater heater;
	
	public static void fillLocator() {
		thermostatLocator.put("28:26:1c:60:07:00:00:ad", "Cottage - Outdoors");
		thermostatLocator.put("28:ff:2c:31:44:04:00:c2", "Cottage - Indoors");
		thermostatLocator.put("28:4d:4a:60:07:00:00:9f", "Home - Outdoors ESP8266");
		thermostatLocator.put("28:db:b1:1f:06:00:00:d3", "I.D.E.A - ESP8266");
		
		heater = new WaterHeater();
		
	}
	
}
