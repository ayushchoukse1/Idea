package com.idea.spark;

import java.net.UnknownHostException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.glassfish.grizzly.http.util.TimeStamp;

import com.idea.mongodb.MongoDBConnection;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class PersistData {

	public static void persistLightData() {
		// Hashmap will be stored in MongoDB here
		//System.out.println("Test: presisting light data in mongo");
		DBCollection table = null;
		DBCollection newLightsTable = null;
		DB db = null;
		try {
			db = MongoDBConnection.getDBConnection();
			table = db.getCollection("lights");
			newLightsTable = db.getCollection("newlights");
			if(db == null)
				System.out.println("Mongo Null ***");
		} catch (Exception e) {
			e.printStackTrace();
		}
		BasicDBObject query = new BasicDBObject();
		BasicDBObject retreivalObj = null;
		BasicDBObject updateObj = null;
		DBCursor cursor = null;
		long onTime = 0;
		long wasteTime = 0;
		long timeStamp = 0;
		String bulbName = null;
		//System.out.println("Mongo confg ok...");
		Iterator itr = ProcessUtility.lightsMap.entrySet().iterator();
		while (itr.hasNext()) {
			
			//System.out.println("In iterator");
			// find the bulb entry in mongo
			Map.Entry<String, Lighting> pair = (Map.Entry) itr.next();
			bulbName = pair.getKey();
			onTime = pair.getValue().getOnTime().getTime();
			wasteTime = pair.getValue().getWasteTime().getTime();
			timeStamp = pair.getValue().getTimestamp().getTime();
			// System.out.println("bulbName = " + bulbName + " onTimeCurrent = "
			// + onTime + " wasteTime = " + wasteTime);

			/* Storing in "lights" Collection in mongodb */

			// keep a document ready with the name add the time diff later
			BasicDBObject newDoc = new BasicDBObject();
			newDoc.put("name", bulbName);
			newDoc.put("deviceId", pair.getValue().getLightID());
			newDoc.put("onTime", onTime);
			newDoc.put("onTimeHour", new SimpleDateFormat("HH:mm:ss").format(new Date(onTime)));
			newDoc.put("wasteTimeHour", new SimpleDateFormat("HH:mm:ss").format(new Date(wasteTime)));
			newDoc.put("wasteTime", wasteTime);
			newDoc.put("timestamp", timeStamp);

			table.insert(newDoc);
			
			/* Storing in "newlights" Collection in mongodb */

			Calendar zeroTime = Calendar.getInstance();
			zeroTime.set(Calendar.HOUR, 0);
			zeroTime.set(Calendar.MINUTE, 0);
			zeroTime.set(Calendar.SECOND, 0);
			zeroTime.set(Calendar.MILLISECOND, 0);
			zeroTime.set(Calendar.HOUR_OF_DAY, 0);
			long zeroTimeMilli = new Timestamp(zeroTime.getTimeInMillis()).getTime();
			long onTimeUsage = onTime - zeroTimeMilli;
			long wasteTimeUsage = wasteTime - zeroTimeMilli;
			long timePassed = timeStamp - zeroTimeMilli;
			double onTimePercent = ((double) onTimeUsage / timePassed) * 100;
			double wasteTimePercent = ((double) wasteTimeUsage / timePassed) * 100;
			double onTimeUsageWatts = ((double) (onTimeUsage / (60 * 60 * 1000)) * (8.5 / 100));
			double wasteTimeUsageWatts = ((double) (wasteTimeUsage / (60 * 60 * 1000)) * (8.5 / 100));

			BasicDBObject newLightsDoc = new BasicDBObject();
			newLightsDoc.put("name", bulbName);
			newLightsDoc.put("deviceId", pair.getValue().getLightID());
			newLightsDoc.put("onTimeInPercent", onTimePercent);
			newLightsDoc.put("onTimeUsageInWatts", onTimeUsageWatts);
			newLightsDoc.put("onTimeInHours", new SimpleDateFormat("HH:mm:ss").format(new Date(onTime)));
			newLightsDoc.put("wasteTimeInPercent", wasteTimePercent);
			newLightsDoc.put("wasteTimeUsageInWatts", wasteTimeUsageWatts);
			newLightsDoc.put("wasteTimeInHours", new SimpleDateFormat("HH:mm:ss").format(new Date(wasteTime)));
			newLightsDoc.put("date", Calendar.getInstance().getTime());
			newLightsDoc.put("timestamp", timeStamp);

			query.put("name", bulbName);
			cursor = newLightsTable.find(query);

			if (cursor.count() == 0) {
				newLightsTable.insert(newLightsDoc);
			} else {
				retreivalObj = (BasicDBObject) cursor.next();
				updateObj = new BasicDBObject();
				updateObj.put("$set", newLightsDoc);
				newLightsTable.update(query, updateObj);
			}
			System.out.println("MONGO : Stored in mongodb " + newLightsDoc.toJson());
		}
	}

	public static void persistTempRecomm(String recomm, String deviceId, String location, Double diff, String title, double currentTemp) {
		DBCollection table = null;
		
		DB db = null;
		try {
			db = MongoDBConnection.getDBConnection();
			table = db.getCollection("tempRecomms");
			
			if(db == null)
				System.out.println("Mongo Null ***");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		BasicDBObject newDoc = new BasicDBObject();
		
		Timestamp currentTime = new Timestamp((new java.util.Date()).getTime());
		
		newDoc.put("deviceId", deviceId);
		newDoc.put("location", location);
		newDoc.put("diff", diff);
		newDoc.put("recommendation", recomm);
		newDoc.put("timestamp", currentTime);
		newDoc.put("title", title);
		newDoc.put("currentTemp", currentTemp);
		
		BasicDBObject query = new BasicDBObject();
		BasicDBObject retreivalObj = null;
		BasicDBObject updateObj = null;
		DBCursor cursor = null;
		query.put("deviceId", deviceId);
		cursor = table.find(query);
		if (cursor.count() == 0) {
			table.insert(newDoc);
		} else {
			retreivalObj = (BasicDBObject) cursor.next();
			updateObj = new BasicDBObject();
			updateObj.put("$set", newDoc);
			table.update(query, updateObj);
		}
		
	}

	public static void persistTempAction(String action, String deviceId, String title, double currentTemp) {
		
		DBCollection table = null;
		
		DB db = null;
		try {
			db = MongoDBConnection.getDBConnection();
			table = db.getCollection("tempActions");
			
			if(db == null)
				System.out.println("Mongo Null ***");
		} catch (Exception e) {
			e.printStackTrace();
		}
		BasicDBObject newDoc = new BasicDBObject();
		Timestamp currentTime = new Timestamp((new java.util.Date()).getTime());
		newDoc.put("deviceId", deviceId);
		newDoc.put("action", action);
		newDoc.put("timestamp", currentTime);
		newDoc.put("title", title);
		newDoc.put("currentTemp", currentTemp);
		
		BasicDBObject query = new BasicDBObject();
		BasicDBObject updateObj = null;
		DBCursor cursor = null;
		query.put("deviceId", deviceId);
		cursor = table.find(query);
		if (cursor.count() == 0) {
			System.out.println("TEST : First Time Insert");
			table.insert(newDoc);
		} else {
			System.out.println("TEST : UPDATE");
			updateObj = new BasicDBObject();
			updateObj.put("$set", newDoc);
			table.update(query, updateObj);
		}
		
	}
}
