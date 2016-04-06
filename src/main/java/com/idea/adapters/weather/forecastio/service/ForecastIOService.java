package com.idea.adapters.weather.forecastio.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;

public class ForecastIOService {

	public  final String FORECASTIO_API_KEY = "338ca7cbf5989fdad923f81b998255d3";
	public  final String GEOCODING_API_KEY = "AIzaSyBZknovrESFLOUBTgk_PBkvtvHp_T-QVLM";
	public  final String FORECASTIO_BASE_URL = "https://api.forecast.io/forecast/";
	public  final String GEOCODING_BASE_URL = "https://maps.googleapis.com/maps/api/geocode/";
	public  final String REQUEST_TYPE = "GET";

	protected  String urlStringBuilder(double latitude, double longitude) {
		/*
		 * Sample url
		 * https://api.forecast.io/forecast/338ca7cbf5989fdad923f81b998255d3/37.
		 * 8267,-122.423
		 */
		StringBuilder urlString = new StringBuilder(FORECASTIO_BASE_URL);
		urlString.append(FORECASTIO_API_KEY).append("/").append(latitude).append(",").append(longitude);
		return urlString.toString();

	}

	protected  HttpURLConnection connectionBuilder(String urlString) throws IOException {
		URL url = new URL(urlString);
		return (HttpURLConnection) url.openConnection();
	}

	protected  String getRequestedCode(HttpURLConnection con) throws IOException {
		InputStream is = con.getInputStream();
		BufferedReader urlReader = new BufferedReader(new InputStreamReader(is));
		StringWriter writer = new StringWriter();
		String s = "";
		while ((s = urlReader.readLine()) != null) {
			writer.write(s);
		}
		writer.close();
		is.close();
		return writer.toString();
	}

	protected  String geocodingStringBuilder(String address, String key) throws UnsupportedEncodingException {
		String responseType = "json";
		StringBuilder coordinatesUrlString = new StringBuilder(GEOCODING_BASE_URL);
		coordinatesUrlString.append(responseType).append("?").append("address=")
				.append(java.net.URLEncoder.encode(address, "UTF-8"));
		coordinatesUrlString.append(java.net.URLEncoder.encode(key, "UTF-8"));
		return coordinatesUrlString.toString();
	}

	public Double[] getCoordinates(String address) throws Exception {
		/*
		 * Sample Url
		 * https://maps.googleapis.com/maps/api/geocode/json?address=1600+
		 * Amphitheatre+Parkway,+Mountain+View,+CA&key=YOUR_API_KEY
		 */
		Double[] coord = new Double[2];
		GeoApiContext context = new GeoApiContext().setApiKey(GEOCODING_API_KEY);
		GeocodingResult[] results = GeocodingApi.geocode(context, address).await();
		coord[0] = results[0].geometry.location.lat;
		coord[1] = results[0].geometry.location.lng;

		return coord;
	}
	
	public String getWeatherForecast(String address) throws Exception{
		Double[] coord = getCoordinates(address);
		return getRequestedCode(connectionBuilder(urlStringBuilder(coord[0], coord[1])));
	}

}
