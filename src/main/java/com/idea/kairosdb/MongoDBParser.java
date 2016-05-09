package com.idea.kairosdb;

import org.codehaus.jettison.json.JSONObject;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.idea.mongodb.MongoDBConnection;
import com.idea.util.Util;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDBParser implements TopicParserMetric {
	private static final Logger LOG = LoggerFactory.getLogger(MongoDBParser.class);

	public void parseTopic(String topic, JSONObject jsonObject) {
		LOG.debug("In **MONGODB** parser : " + "JSON : " + jsonObject.toString());
		//Make MongoDB connection and add the data to the different collections
		//collections:['devices', 'lights', 'thermostats', 'temperature-active']
		try {
			//BasicDBObject dbObject = new BasicDBObject(
			DBObject dbObject = Util.encode(jsonObject);
			
			DBCollection collection = null;
			if(jsonObject != null){
				if(jsonObject.has("deviceId")){
					//dump to devices collection
					//get collection instance
					dbObject.put("timestamp", System.currentTimeMillis());
					collection = MongoDBConnection.getCollection("devices");
				}else if (jsonObject.has("client") && jsonObject.getString("client").equals("I.D.E.A. Lighting")){
					//dump to lights collection
					//get collection instance
					dbObject.put("timestamp", System.currentTimeMillis());
					collection = MongoDBConnection.getCollection("lights");
				}else if (jsonObject.has("manufacturer") && jsonObject.getString("manufacturer").equals("Honeywell")
						&& jsonObject.getString("client").equals("IDEA Thermostat")) {
					//dump to thermostat collection
					//get collection instance
					collection = MongoDBConnection.getCollection("thermostats");
				}else if(jsonObject.has("latitude") && jsonObject.has("longitude") && jsonObject.has("timezone")){
					collection = MongoDBConnection.getCollection("active-temperature");
				}
				if(collection != null)
					collection.insert(dbObject);
			}
		} catch (Exception e) {
			System.out.println("Error in MongoDBParser : " + e.getMessage());
		}
	}

	public void setPropertyName(String name) {

	}

}
