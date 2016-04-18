package com.idea.spark;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.io.FileWriteMode;
import com.idea.adapters.weather.forecastio.service.*;

public class ForecastTrial {
	
	public static void main(String[] args)
	{
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
		System.out.println(forecastTemp);
	}

}
