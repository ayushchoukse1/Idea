package com.idea.spark;

import java.net.UnknownHostException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class PersistData {

	public static void persistLightData() {
		// Hashmap will be stored in MongoDB here
		try {
			ProcessUtility.mongo = new MongoClient("localhost", 27017);
			ProcessUtility.db = ProcessUtility.mongo.getDB("ideadb");
			ProcessUtility.table = ProcessUtility.db.getCollection("lights");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		BasicDBObject query = new BasicDBObject();
		DBCursor cursor = null;
		long onTime = 0;
		long wasteTime = 0;
		long timeStamp = 0;
		String bulbName = null;
		Iterator itr = ProcessUtility.lightsMap.entrySet().iterator();
		while (itr.hasNext()) {

			// find the bulb entry in mongo
			Map.Entry<String, Lighting> pair = (Map.Entry) itr.next();
			bulbName = pair.getKey();
			onTime = pair.getValue().getOnTime().getTime();
			wasteTime = pair.getValue().getWasteTime().getTime();
			timeStamp = pair.getValue().getTimestamp().getTime();
			// System.out.println("bulbName = " + bulbName + " onTimeCurrent = "
			// + onTime + " wasteTime = " + wasteTime);
			query.put("name", bulbName);
			cursor = ProcessUtility.table.find(query);

			// keep a document ready with the name add the time diff later
			BasicDBObject newDoc = new BasicDBObject();
			newDoc.put("name", bulbName);
			newDoc.put("onTime", onTime);
			newDoc.put("onTimeHour", new SimpleDateFormat("HH:mm:ss").format(new Date(onTime)));
			newDoc.put("wasteTimeHour", new SimpleDateFormat("HH:mm:ss").format(new Date(wasteTime)));
			newDoc.put("wasteTime", wasteTime);
			newDoc.put("timestamp", timeStamp);
			// ontime and wastetime in hours
			ProcessUtility.table.insert(newDoc);
		}
	}

	public static void persistTempRecomm(String recomm, String deviceID, String location, Double diff) {
		try {
			ProcessUtility.mongo = new MongoClient("localhost", 27017);
			ProcessUtility.db = ProcessUtility.mongo.getDB("ideadb");
			ProcessUtility.table = ProcessUtility.db.getCollection("tempRecomms");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		BasicDBObject newDoc = new BasicDBObject();
		Timestamp currentTime = new Timestamp((new java.util.Date()).getTime());
		newDoc.put("Timestamp", currentTime);
		newDoc.put("DeviceID", deviceID);
		newDoc.put("Location", location);
		newDoc.put("Diff", diff);
		newDoc.put("Recommendation", recomm);
		ProcessUtility.table.insert(newDoc);
	}

	public static void persistTempAction(String action, String deviceID) {
		try {
			ProcessUtility.mongo = new MongoClient("localhost", 27017);
			ProcessUtility.db = ProcessUtility.mongo.getDB("ideadb");
			ProcessUtility.table = ProcessUtility.db.getCollection("tempActions");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		BasicDBObject newDoc = new BasicDBObject();
		Timestamp currentTime = new Timestamp((new java.util.Date()).getTime());
		newDoc.put("Timestamp", currentTime);
		newDoc.put("DeviceID", deviceID);
		newDoc.put("Action", action);
		ProcessUtility.table.insert(newDoc);
	}
}
