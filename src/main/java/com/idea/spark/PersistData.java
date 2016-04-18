package com.idea.spark;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class PersistData {
	
	public static void persistLightData(){
		//Hashmap will be stored in MongoDB here
		
		try {
			ProcessUtility.mongo = new MongoClient( "localhost" , 27017 );
			ProcessUtility.db = ProcessUtility.mongo.getDB("ideadb");
			ProcessUtility.table = ProcessUtility.db.getCollection("lights");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		BasicDBObject query = new BasicDBObject();
		BasicDBObject newDoc = new BasicDBObject();
		BasicDBObject retreivalObj = null;
		BasicDBObject updateObj = null;
		DBCursor cursor = null;
		long onTimeOld = 0;
		long onTimeCurrent = 0;
		String bulbName = null;
		
		Iterator itr = ProcessUtility.lightsMap.entrySet().iterator();
		
		while(itr.hasNext())
		{
			//find the bulb entry in mongo
			Map.Entry<String, Lighting> pair = (Map.Entry)itr.next();
			bulbName = pair.getKey();
			onTimeCurrent = pair.getValue().getOnTime().getTime();
			System.out.println("bulbName = "+ bulbName + " onTimeCurrent = "+ onTimeCurrent);
			query.put("name", bulbName);
			cursor = ProcessUtility.table.find(query);
			
			//keep a document ready with the name add the time diff later
			newDoc.put("name", bulbName);
			
			
			if(cursor.count() == 0)
			{
				//insert
				newDoc.put("onTime", (onTimeOld + onTimeCurrent));
				ProcessUtility.table.insert(newDoc);
				
			}
			else
			{
				//retrieval
				retreivalObj = (BasicDBObject)cursor.next();
				onTimeOld = retreivalObj.getLong("onTime");
				newDoc.put("onTime", (onTimeOld + onTimeCurrent));
				
				//update
				updateObj = new BasicDBObject();
				updateObj.put("$set", newDoc);
				ProcessUtility.table.update(query, updateObj);
				
			}
		}
		
	}
	

}
