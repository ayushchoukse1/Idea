package com.idea.spark;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ProcessLightLines {
	
	public static void readLightRDD(JavaDStream<String> dStream) {


		dStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {
					/*
					 * Make modifications to the String here.
					 */
					@Override
					public String call(String string) throws Exception {
						//checkUpdate(string);
						System.out.println("checkUpdate running for: "+string);
						JSONObject jobj = new JSONObject(string);
						String name = jobj.getString("name");
						String currentState = jobj.getString("state");
						Timestamp timestamp = Timestamp.valueOf(jobj.getString("TimeStamp"));
						Calendar temp = Calendar.getInstance();
						temp.set(Calendar.HOUR, 0);
						temp.set(Calendar.MINUTE, 0);
						temp.set(Calendar.SECOND, 0);
						temp.set(Calendar.MILLISECOND, 0);
						Timestamp onTime = new Timestamp(temp.getTimeInMillis());
						if (ProcessUtility.lightsMap.containsKey(name)) {
							System.out.println("objectHashmap has " + name);
							Lighting light = ProcessUtility.lightsMap.get(name);
							String initialState = light.getIntialState();
							if (!initialState.equals(currentState)) {
								System.out.println("State changed for "+ name);
								if (initialState.equals("Red") && currentState.equals("Green")) {

									/*
									 * State changed from Red to Green update initialState, and
									 * store the timeStamp
									 * 
									 */

									System.out.println(light.getName()+" has changed from red to green at: "+ light.getTimestamp().getTime());
									light.setTimestamp(timestamp);
									light.setIntialState(currentState);

								} else if (initialState.equals("Green") && currentState.equals("Red")) {

									/*
									 * State changed from Green to Red update timestamp to new
									 * timestamp, update Ontime for light, update initialState.
									 */

									// The total time for which the light was on.
									long diff = light.getTimestamp().getTime() - timestamp.getTime();
									long oldTimeInMilli = light.getOnTime().getTime();
									oldTimeInMilli = oldTimeInMilli + diff;
									light.setOnTime(new Timestamp(oldTimeInMilli));
									System.out.println("New Updated onTime for "+light.getName()+" is "+light.getOnTime().getTime());
									light.setTimestamp(timestamp);
									light.setIntialState(currentState);
								}

							}
							else{
								System.out.println("No Change of state in "+name);
							}
						} 
						else {
							Lighting light = new Lighting();
							light.setName(name);
							light.setOnTime(onTime);
							light.setTimestamp(timestamp);
							light.setIntialState(currentState);
							ProcessUtility.lightsMap.put(name, light);
							System.out.println();
							System.out.println("Light added: "+light.getName()+" with onTime: "+light.getOnTime().getTime()+" to ObjectHashmap");
						}
						return string;
					}
				});
				
				//print objecHashmap
				/*for (Map.Entry<String, Lighting> entry : objectHashmap.entrySet()) {
				    System.out.println(entry.getKey()+" : "+entry.getValue());
				}*/
				List<String> ls = rowRDD.collect();
				ObjectMapper mapper = new ObjectMapper();
				for (int i = 0; i < ls.size(); i++) {
					/*
					 * 
					 * Printing the RDD's as JSON Object
					 * 
					 */
					Object json = mapper.readValue(ls.get(i), Object.class);
					mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
					//System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
				}
				return null;
			}
		});
	}

	public static void checkUpdate(String string) throws JSONException {
		/*
		 * 1. Convert string to json 
		 * 2. extract the name. 
		 * 3. check if the name is in HashMap. 
		 * 		3.1 if not --> create new object(Lighting class) of 
		 *  		that name and initialize with default values. 
		 *  	3.2 if yes --> go to step 4. 
		 * 4. check the state of light with initialState of light
		 * object. 
		 * 		4.1 If state has not changed --> do nothing. 
		 * 		4.2 If state has changed --> Go to step 5. 
		 * 5. Check for the following cases: 
		 * 5.1 If state changed from Red --> Green 
		 * 	update initialState, and store the timeStamp 
		 * 5.2 If state changed from Green --> Red update timestamp to
		 * new timestamp, update Ontime for light, update initialState.
		 * 
		 */
		System.out.println("checkUpdate running for: "+string);
		JSONObject jobj = new JSONObject(string);
		String name = jobj.getString("name");
		String currentState = jobj.getString("state");
		Timestamp timestamp = Timestamp.valueOf(jobj.getString("Timestamp"));
		Calendar temp = Calendar.getInstance();
		temp.set(Calendar.HOUR, 0);
		temp.set(Calendar.MINUTE, 0);
		temp.set(Calendar.SECOND, 0);
		temp.set(Calendar.MILLISECOND, 0);
		Timestamp onTime = new Timestamp(temp.getTimeInMillis());
		if (!ProcessUtility.lightsMap.containsKey(name)) {
			Lighting light = new Lighting();
			light.setName(name);
			light.setOnTime(onTime);
			light.setTimestamp(timestamp);
			light.setIntialState(currentState);
			ProcessUtility.lightsMap.put(name, light);
			System.out.println("Light added: "+light.getName()+" with onTime: "+light.getOnTime().getTime());
		} else {
			Lighting light = ProcessUtility.lightsMap.get(name);
			String initialState = light.getIntialState();
			if (!initialState.equals(currentState)) {
				if (initialState.equals("Red") && currentState.equals("Green")) {
	
					/*
					 * State changed from Red to Green update initialState, and
					 * store the timeStamp
					 * 
					 */
	
					light.setTimestamp(timestamp);
					light.setIntialState(currentState);
	
				} else if (initialState.equals("Green") && currentState.equals("Red")) {
	
					/*
					 * State changed from Green to Red update timestamp to new
					 * timestamp, update Ontime for light, update initialState.
					 */
	
					// The total time for which the light was on.
					long diff = light.getTimestamp().getTime() - timestamp.getTime();
					long oldTimeInMilli = light.getOnTime().getTime();
					oldTimeInMilli = oldTimeInMilli + diff;
					
					light.setOnTime(new Timestamp(oldTimeInMilli));
					System.out.println("New Updated onTime for "+light.getName()+" is "+light.getOnTime().getTime());
					light.setTimestamp(timestamp);
					light.setIntialState(currentState);
				}
	
			}
		}
	}


}
