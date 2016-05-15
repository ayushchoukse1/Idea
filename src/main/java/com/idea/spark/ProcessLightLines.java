package com.idea.spark;

import java.sql.Timestamp;
import java.util.Calendar;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ProcessLightLines implements java.io.Serializable {

	public void processLightString(String string) throws Exception {

		// 1. Convert string to json
		// 2. extract the name.
		// 3. check if the name is in HashMap.
		// 3.1 if not --> create new object(Lighting class) of that name and
		// initialize with default values.
		// 3.2 if yes --> go to step 4.
		// 4. check the state of light with initialState of light object.
		// 4.1 If state has not changed --> do nothing.
		// 4.2 If state has changed --> Go to step 5.
		// 5. Check for the following cases:
		// 5.1 If state changed from Red --> Green
		// update initialState, and store the timeStamp
		// 5.2 If state changed from Green --> Red
		// update timestamp to new timestamp, update Ontime for light, update
		// initialState.
		System.out.println("Test: Processing Light lines");
		String name = null;
		String currentState = null;
		Timestamp timestamp = null;
		try {
			JSONObject jobj = new JSONObject(string);
			System.out.println("Test Light String: " + string);
			name = jobj.getString("name");
			currentState = jobj.getString("state");
			//timestamp = Timestamp.valueOf(jobj.getString("TimeStamp"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(timestamp == null)
		{
			timestamp = new Timestamp((new java.util.Date()).getTime());
		}
		Calendar temp = Calendar.getInstance();
		temp.set(Calendar.HOUR, 0);
		temp.set(Calendar.MINUTE, 0);
		temp.set(Calendar.SECOND, 0);
		temp.set(Calendar.MILLISECOND, 0);
		temp.set(Calendar.HOUR_OF_DAY, 0);
		Timestamp onTime = new Timestamp(temp.getTimeInMillis());
		Timestamp wasteTime = new Timestamp(temp.getTimeInMillis());
		System.out.println("Test Light found: " + name);
		if (name != null && !ProcessUtility.lightsMap.containsKey(name)) {
			Lighting light = new Lighting();
			light.setName(name);
			light.setOnTime(onTime);
			light.setWasteTime(wasteTime);
			light.setTimestamp(timestamp);
			light.setIntialState(currentState);
			ProcessUtility.lightsMap.put(name, light);
			System.out.println("LIGHTS : Light added: " + light.getName() + " with onTime: " + light.getOnTime()
					+ " with wasteTime: " + light.getWasteTime() + " with initialState: " + light.getIntialState()
					+ " with timestamp: " + light.getTimestamp());
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
//					System.out.println("LIGHTS : BEFORE Light update: " + light.getName() + " with onTime: "
//							+ light.getOnTime() + " with wasteTime: " + light.getWasteTime() + " with initialState: "
//							+ light.getIntialState() + " with timestamp: " + light.getTimestamp());
					
					// The total time for which the light was on.
					long currentTime = timestamp.getTime();
					long initialTime = (light.getTimestamp().getTime());
					long diff = currentTime - initialTime;
					long oldTimeInMilli = light.getOnTime().getTime();
					oldTimeInMilli = oldTimeInMilli + diff;
					light.setOnTime(new Timestamp(oldTimeInMilli));

					System.out
							.println("LIGHTS : New Updated onTime for " + light.getName() + " is " + light.getOnTime());
					light.setTimestamp(timestamp);
					light.setIntialState(currentState);

					// calculating waste
					long sunriseTime = ExternalData.sunriseTime;
					long sunsetTime = ExternalData.sunsetTime;
					long oldWasteTime = light.getWasteTime().getTime();

//					System.out.println("LIGHTS : sunriseTime: " + new Timestamp(sunriseTime) + " sunsetTime : "
//							+ new Timestamp(sunsetTime));

					if (initialTime > sunriseTime && currentTime < sunsetTime) {
						oldWasteTime += diff;
					} else if (initialTime < sunriseTime && currentTime > sunsetTime) {
						oldWasteTime = oldWasteTime + (currentTime - sunriseTime);
					} else if (initialTime > sunriseTime && currentTime < sunsetTime) {
						oldWasteTime = oldWasteTime + (sunsetTime - initialTime);
					} else if (initialTime > sunriseTime && currentTime > sunsetTime) {
						oldWasteTime = oldWasteTime + (sunsetTime - sunriseTime);
					}
					light.setWasteTime(new Timestamp(oldWasteTime));
					
//					System.out.println("LIGHTS : AFTER Light update: " + light.getName() + " with onTime: "
//							+ light.getOnTime() + " with wasteTime: " + light.getWasteTime() + " with initialState: "
//							+ light.getIntialState() + " with timestamp: " + light.getTimestamp());
				}
			}
		}
	}
}
