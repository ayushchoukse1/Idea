package com.idea.spark;

import java.io.Serializable;

import org.codehaus.jettison.json.JSONObject;

public class ProcessTempLines implements Serializable {

	public void readTempRDD(String string) throws Exception {

		System.out.println("TEMPERATURE : Checking temperature lines");

		JSONObject jsonObj = new JSONObject(string);
		String deviceID = jsonObj.getString("deviceId");
		String location = ProcessUtility.thermostatLocator.get(deviceID);
		Double currentTemp = jsonObj.getDouble("temperature");
		Double forecastTempCA = ExternalData.getForecastTemp("1600+Amphitheatre+Parkway,+Mountain+View,+CA");
		Double forecastTempCO = ExternalData.getForecastTemp("30+E+Pikes+Peak+Ave,+Colorado+Springs,+CO");
		Double forecastTemp = 0.0;
		Double tempDiff = 0.0;
		if(deviceID.contains("28:4d:4a:60:07:00:00:9f"))
			{
				tempDiff = forecastTempCO - currentTemp;
				forecastTemp = forecastTempCO;
			}
		else 
			{
				tempDiff= forecastTempCA - currentTemp;
				forecastTemp = forecastTempCA;
			}
		String recomm = null;
		String title = "The forecasted temperature is " + forecastTemp + "F";
		System.out.println("TEMPERATURE : Forecasted Temperature: " + forecastTempCA);

		if (tempDiff < 0) {
			// its becoming cold so increase the temperature
			System.out.println("TEMPERATURE : Device: " + location + " is at: " + currentTemp
					+ "F should decrease the temperature by " + Math.abs(tempDiff) + "F");
			 recomm = "Decrease the temperature by " + Math.abs(tempDiff) + "F";
			 System.out.println(recomm+" " +deviceID+" " +location+" " + tempDiff+" " + title+" " +currentTemp);
			PersistData.persistTempRecomm(recomm, deviceID, location, tempDiff, title, currentTemp);
		} else if (tempDiff > 0) {
			// its becoming hotter
			System.out.println("TEMPERATURE : Device at " + location + " is at: " + currentTemp
					+ "F should increase the temperature by " + Math.abs(tempDiff) + "F");
			recomm = "Increase the temperature by " + Math.abs(tempDiff) + "F";
			
		} else {
			System.out.println("TEMPERATURE : Temperature remains same");
		}
		
		if(recomm != null){
			System.out.println(recomm+" " +deviceID+" " +location+" " + tempDiff+" " + title+" " +currentTemp);
			PersistData.persistTempRecomm(recomm, deviceID, location, tempDiff, title, currentTemp);	
		}
		/*
		 * Find the difference between outdoors thermostat temp and the actual
		 * current temp. This give us the difference in temperature between city
		 * and mountain temp
		 */

		Double diffAccuracy = 0.0;
		Double currentExternalTemp = ExternalData.forecastTempCurr;
		Double forcastAccurate = 0.0;
		Double pipeLowerThreshold = 40.0;

		if (location == "Cottage - Outdoors") {

			String action = null;
			diffAccuracy = currentExternalTemp - currentTemp;
			forcastAccurate = forecastTempCA - diffAccuracy;

			System.out.println("TEMPERATURE : Water Heater CurTemp: " + currentTemp + " CurExtTemp: "
					+ currentExternalTemp + " forecastTemp: " + forecastTempCA + " ForAccTemp: " + forcastAccurate);

			if (forcastAccurate <= pipeLowerThreshold && (!ProcessUtility.heater.isState())) {
				action = "Water heater is switched on";
				ProcessUtility.heater.setState(true);
			} else if (forcastAccurate >= pipeLowerThreshold && ProcessUtility.heater.isState()) {
				action = "Water heater is switched off";
				ProcessUtility.heater.setState(false);
			}

			if (action != null) {
				System.out.println("TEMPERATURE : Action = " + action);
				PersistData.persistTempAction(action, deviceID, title, currentTemp);
			}
		}

	}
}
