package com.idea.spark;

import java.sql.Timestamp;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.idea.adapters.weather.forecastio.service.ForecastIOService;

public class ExternalData {

	private static final String location = "1600+Amphitheatre+Parkway,+Mountain+View,+CA";
	static long sunriseTime;
	static long sunsetTime;
	static Timestamp current = new Timestamp((new java.util.Date()).getTime());
	static long currentDate = (current.getTime()) / 1000;
	static Double forecastTemp = 0.0;
	public static void setSunTime(){
		
		sunsetTime = getSunsetTime();
		sunriseTime = getSunriseTime();
		forecastTemp = getForecastTemp();
	}
	
	public static long getSunsetTime() {
		ForecastIOService fs = new ForecastIOService();
		try {
			String forecast = fs.getWeatherForecast(location);
			JSONObject jsonObj = new JSONObject(forecast);
			jsonObj = jsonObj.getJSONObject("daily");
			JSONArray arr = jsonObj.getJSONArray("data");
			int i = 0;
			for (; i < arr.length(); i++) {
				long obtainedDate = arr.getJSONObject(i).getLong("time");
				if (Math.abs(obtainedDate - currentDate) <= 86400) {
					sunsetTime = arr.getJSONObject(i).getLong("sunsetTime");
					break;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return sunsetTime * 1000;
	}

	public static long getSunriseTime() {
		ForecastIOService fs = new ForecastIOService();
		try {
			String forecast = fs.getWeatherForecast(location);
			JSONObject jsonObj = new JSONObject(forecast);
			jsonObj = jsonObj.getJSONObject("daily");
			JSONArray arr = jsonObj.getJSONArray("data");
			int i = 0;
			for (; i < arr.length(); i++) {
				long obtainedDate = arr.getJSONObject(i).getLong("time");
				if (Math.abs(obtainedDate - currentDate) <= 86400) {
					sunriseTime = arr.getJSONObject(i).getLong("sunriseTime");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sunriseTime * 1000;
	}

	public static double getForecastTemp() {
		ForecastIOService fs = new ForecastIOService();
		Double forecastTemp = 0.0;
		try {
			String forecast = fs.getWeatherForecast("1600+Amphitheatre+Parkway,+Mountain+View,+CA");
			JSONObject jsonObj = new JSONObject(forecast);
			jsonObj = jsonObj.getJSONObject("hourly");
			JSONArray arr = jsonObj.getJSONArray("data");
			forecastTemp = arr.getJSONObject(3).getDouble("temperature");
		} catch (Exception e) {
			return 56.1;
			
		}
		return forecastTemp;
		 
	}

	public static double getCurrentExternalTemp() {
		ForecastIOService fs = new ForecastIOService();
		Double forecastTemp = 0.0;
		try {
			String forecast = fs.getWeatherForecast("1600+Amphitheatre+Parkway,+Mountain+View,+CA");
			JSONObject jsonObj = new JSONObject(forecast);
			jsonObj = jsonObj.getJSONObject("hourly");
			JSONArray arr = jsonObj.getJSONArray("data");
			forecastTemp = arr.getJSONObject(0).getDouble("temperature");
		} catch (Exception e) {
			 return 60;
		}
		return forecastTemp;
		// return 60;
	}

}
