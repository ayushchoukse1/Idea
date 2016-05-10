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
		Double forecastTemp = SparkProcess.getForecastTemp();
		System.out.println("TEMPERATURE : Forecasted Temperature: " + forecastTemp);
		Double tempDiff = forecastTemp - currentTemp;
		if (tempDiff < 0) {
			// its becoming cold so increase the temperature
			System.out.println("TEMPERATURE : Device: " + location + " is at: " + currentTemp
					+ "F should decrease the temperature by " + Math.abs(tempDiff) + "F");
		} else if (tempDiff > 0) {
			// its becoming hotter
			System.out.println("TEMPERATURE : Device at " + location + " is at: " + currentTemp
					+ "F should increase the temperature by " + Math.abs(tempDiff) + "F");
		} else {
			System.out.println("TEMPERATURE : Temperature remains same");
		}

		/*
		 * Find the difference between outdoors thermostat temp and the actual
		 * current temp. This give us the difference in temperature between city
		 * and mountain temp
		 */

		Double diffAccuracy = 0.0;
		Double currentExternalTemp = SparkProcess.getCurrentExternalTemp();
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
