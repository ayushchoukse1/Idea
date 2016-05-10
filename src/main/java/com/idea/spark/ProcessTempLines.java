package com.idea.spark;

import java.io.Serializable;

import org.codehaus.jettison.json.JSONObject;

public class ProcessTempLines implements Serializable {
<<<<<<< HEAD
=======
	
	
>>>>>>> 440ae4d1d218aaf59dc34056e8f7fec8fcbca238

	public void readTempRDD(String string) throws Exception {
		System.out.println("TEMPERATURE : Checking temperature lines");
		JSONObject jsonObj = new JSONObject(string);
		String deviceID = jsonObj.getString("deviceId");
<<<<<<< HEAD
		String location = ProcessUtility.thermostatLocator.get(deviceID);
		Double currentTemp = jsonObj.getDouble("temperature");
		Double forecastTemp = ExternalData.getForecastTemp();
		System.out.println("TEMPERATURE : Forecasted Temperature: " + forecastTemp);
=======
		//System.out.print("Reading data for device: " + deviceID);
		String location = ProcessUtility.thermostatLocator.get(deviceID);
		//System.out.println(" at location: " + location);
		Double currentTemp = jsonObj.getDouble("temperature"); // thermostat temperature
		//System.out.println("Current Temperature: " + currentTemp);
		Double forecastTemp = ExternalData.getForecastTemp();
		System.out.println("Forecasted Temperature: " + forecastTemp);
>>>>>>> 440ae4d1d218aaf59dc34056e8f7fec8fcbca238
		Double tempDiff = forecastTemp - currentTemp;
		String recomm = null;
		if (tempDiff < 0) {
			// its becoming cold so increase the temperature
<<<<<<< HEAD
			System.out.println("TEMPERATURE : Device: " + location + " is at: " + currentTemp
					+ "F should decrease the temperature by " + Math.abs(tempDiff) + "F");
		} else if (tempDiff > 0) {
			// its becoming hotter
			System.out.println("TEMPERATURE : Device at " + location + " is at: " + currentTemp
					+ "F should increase the temperature by " + Math.abs(tempDiff) + "F");
		} else {
			System.out.println("TEMPERATURE : Temperature remains same");
=======
			recomm = "should decrease the temperature by";
			System.out.println("Device: " + location +" is at: " + currentTemp +"F should decrease the temperature by " + Math.abs(tempDiff) + "F");
		} else if (tempDiff > 0) {
			// its becoming hotter
			recomm = "should increase the temperature by";
			System.out.println("Device at " + location +" is at: " + currentTemp +"F should increase the temperature by " + Math.abs(tempDiff) + "F");
		} else {
			System.out.println("Temperature remains same");
		}
		
		if(recomm != null)
		{
			PersistData.persistTempRecomm(recomm, deviceID, location, Math.abs(tempDiff));
		}
		
		//find the difference between outdoors thermostat temp and the actual current temp
		//This give us the difference in temperature between city and mountain temp
		Double diffAccuracy = 0.0;
		Double currentExternalTemp = ExternalData.getCurrentExternalTemp(); // temp obtained from API
		Double forcastAccurate = 0.0;
		Double pipeLowerThreshold = 60.0;
		//Double pipeUpperThreshold = 60.0;
		if(location == "Cottage - Outdoors")
		{
			diffAccuracy = currentExternalTemp - currentTemp;
			forcastAccurate = forecastTemp - diffAccuracy;
			System.out.println("Water Heater CurTemp: " + currentTemp + " CurExtTemp: "+ currentExternalTemp + " forecastTemp: "+ forecastTemp + " ForAccTemp: " + forcastAccurate );
			String action = null;
			if(forcastAccurate <= pipeLowerThreshold && (!ProcessUtility.heater.isState()))
			{
				action = "Water heater is switched on";
				ProcessUtility.heater.setState(true);
				
				//System.out.println("Forecasted Temp: " + forcastAccurate + " Switching heater on...");
			}
			else if(forcastAccurate >= pipeLowerThreshold && ProcessUtility.heater.isState())
			{
				 action = "Water heater is switched off";
				 ProcessUtility.heater.setState(false);
			}
			
			if(action != null)
			{
				System.out.println(action);
				PersistData.persistTempAction(action, deviceID);
			}
>>>>>>> 440ae4d1d218aaf59dc34056e8f7fec8fcbca238
		}
		
		//set a temp limit for out pipes
		
		//check if the the forecasted temp would go out

		/*
		 * Find the difference between outdoors thermostat temp and the actual
		 * current temp. This give us the difference in temperature between city
		 * and mountain temp
		 */

		Double diffAccuracy = 0.0;
		Double currentExternalTemp = ExternalData.getCurrentExternalTemp();
		Double forcastAccurate = 0.0;
		Double pipeLowerThreshold = 40.0;
		// Double pipeUpperThreshold = 60.0;
		if (location == "Cottage - Outdoors") {
			diffAccuracy = currentExternalTemp - currentTemp;
			forcastAccurate = forecastTemp - diffAccuracy;
			System.out.println("TEMPERATURE : Water Heater CurTemp: " + currentTemp + " CurExtTemp: "
					+ currentExternalTemp + " forecastTemp: " + forecastTemp + " ForAccTemp: " + forcastAccurate);
			String action = null;
			if (forcastAccurate <= pipeLowerThreshold && (!ProcessUtility.heater.isState())) {
				action = "Water heater is switched on";
				ProcessUtility.heater.setState(true);
			} else if (forcastAccurate >= pipeLowerThreshold && ProcessUtility.heater.isState()) {
				action = "Water heater is switched off";
				ProcessUtility.heater.setState(false);
			}
			if (action != null) {
				System.out.println("TEMPERATURE : Action = " + action);
				PersistData.persistTempAction(action, deviceID);
			}
		}

	}
}
