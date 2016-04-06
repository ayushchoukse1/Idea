package com.idea.kairosdb;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.StringDataPoint;

public class KairosDBDemo {
	
	private static final Charset UTF8 = Charset.forName("UTF-8");

	public static void main(String[] args) {
		HttpClient myClient = null;
		try {
			myClient = new HttpClient("http://localhost:8080/");
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		//{"client":"IDEA Thermostat","measure":"sys.thermostat.temp","value":"66.0","timestamp":1457700700,
		//"device":"Upstairs","manufacturer":"Honeywell”}
		String jsonString = "{\"client\":\"IDEA Thermostat\",\"measure\":\"sys.thermostat.temp\",\"value\":\"66.0\",\"timestamp\":1457700700,\"device\":\"Upstairs\",\"manufacturer\":\"Honeywell\"}";
		try {
			JSONObject jsonObject = new JSONObject(jsonString);
			if(jsonObject.has("measure")){
				System.out.println("Measure : "+ jsonObject.get("measure"));
			}
		} catch (JSONException e1) {
			e1.printStackTrace();
		}
		
		MetricBuilder builder = MetricBuilder.getInstance();
		builder.addMetric("metric67")
        .addTag("host", "server1")
		.addTag("value","OFF").addTag("device","Upstairs").addTag("manufacturer","Honeywell")
        .addTag("customer", "Acme").addDataPoint(1457700708000L, 19	);
		
 		try {
			System.out.println(myClient.pushMetrics(builder).getStatusCode());

		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
