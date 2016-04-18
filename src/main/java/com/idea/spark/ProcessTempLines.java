package com.idea.spark;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.idea.adapters.weather.forecastio.service.ForecastIOService;

public class ProcessTempLines {
	double forecastTemp;
	public  double getForecastTemp(){
		ForecastIOService fs = new ForecastIOService();
		Double forecastTemp = 0.0;
		try {
			String forecast = fs.getWeatherForecast("1600+Amphitheatre+Parkway,+Mountain+View,+CA");
			JSONObject jsonObj = new JSONObject(forecast);
			jsonObj = jsonObj.getJSONObject("hourly");
			JSONArray arr = jsonObj.getJSONArray("data");
			forecastTemp = arr.getJSONObject(3).getDouble("temperature");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return forecastTemp;
	}

	public  void readTempRDD(JavaDStream<String> dStream){
		System.out.println("Analyzing Temperature data");
		//forecastTemp = getForecastTemp();
		//System.out.println(dStream);
		//System.out.println("Forecasted Temp: " + forecastTemp);
		dStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					/*
					 * Make modifications to the String here.
					 */
					@Override
					public String call(String data) throws Exception {
						
						System.out.println("Checking temperature line....");
						JSONObject jsonObj = new JSONObject(data);
						String deviceID = jsonObj.getString("deviceId");
						System.out.print("Reading data for device: " + deviceID);
						String location = ProcessUtility.thermostatLocator.get(deviceID);
						System.out.println(" at location: " + location);
						Double currentTemp = jsonObj.getDouble("temperature");
						System.out.println("Current Temperature: " + currentTemp);
						System.out.println("Forecasted Temperature: " + forecastTemp);
						Double tempDiff = forecastTemp - currentTemp;
						if(tempDiff < 0)
						{
							//its becoming cold so increase the temperature
							System.out.println("Device at " + location + " should increase the temperature by " + tempDiff + "F");
						}
						else if(tempDiff > 0)
						{
							//its becoming hotter
							System.out.println("Device at " + location + " should decrease the temperature by " + tempDiff + "F");
						}
						else
						{
							System.out.println("Tempaerature remains same");
						}
						
					
						return data;
					}
				});
				
				return null;
			}
		});
			
	}

}
