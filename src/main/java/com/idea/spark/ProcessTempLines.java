package com.idea.spark;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.idea.adapters.weather.forecastio.service.ForecastIOService;

public class ProcessTempLines implements Serializable {
	
	

	public void readTempRDD(String string) throws Exception {
		System.out.println("Checking temperature line....");
		JSONObject jsonObj = new JSONObject(string);
		String deviceID = jsonObj.getString("deviceId");
		//System.out.print("Reading data for device: " + deviceID);
		String location = ProcessUtility.thermostatLocator.get(deviceID);
		//System.out.println(" at location: " + location);
		Double currentTemp = jsonObj.getDouble("temperature"); // thermostat temperature
		//System.out.println("Current Temperature: " + currentTemp);
		Double forecastTemp = ExternalData.getForecastTemp();
		System.out.println("Forecasted Temperature: " + forecastTemp);
		Double tempDiff = forecastTemp - currentTemp;
		String recomm = null;
		if (tempDiff < 0) {
			// its becoming cold so increase the temperature
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
		}
		
		//set a temp limit for out pipes
		
		//check if the the forecasted temp would go out

	}

}
