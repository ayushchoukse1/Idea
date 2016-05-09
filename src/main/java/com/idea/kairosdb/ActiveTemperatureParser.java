package com.idea.kairosdb;

import java.nio.charset.Charset;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveTemperatureParser implements TopicParserMetric {
	private static final Logger LOG = LoggerFactory.getLogger(ActiveTemperatureParser.class);
	private String m_metricName;
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private long timestamp;

	public void parseTopic(String topic, JSONObject jsonObject) {
		LOG.debug("In topic ActiveTemperatureParser");
		if(jsonObject != null){
			try {
				m_metricName = topic;
				
				JSONObject currently = jsonObject.getJSONObject("currently");
				JSONObject daily = jsonObject.getJSONObject("daily");
				JSONObject hourly = jsonObject.getJSONObject("hourly");
				
				JSONObject metricsJson = new JSONObject();
				JSONObject tagsObject = new JSONObject();
				JSONArray dataPoints = new JSONArray();
				metricsJson.put("name", "active_temperature")
						   .put("datapoints", dataPoints)
				           .put("tags", tagsObject);
				
				tagsObject.put("latitude", jsonObject.getString("latitude"))
						  .put("longitude", jsonObject.getString("longitude"))
						  .put("timezone", jsonObject.getString("timezone"))
						  .put("currently.time", currently.getLong("time")*1000)
						  .put("currently.summary", currently.getString("summary"))
						  .put("currently.temperature", currently.getDouble("temperature"));
				
				for(int i = 0; i < daily.getJSONArray("data").length(); i++){
					JSONObject data = daily.getJSONArray("data").getJSONObject(i);
					JSONObject complexObject = new JSONObject();
		
					complexObject.put("summary", data.getString("summary"))
								 .put("icon", data.getString("icon"))
								 .put("temperatureMin", data.getLong("temperatureMin"))
								 .put("temperatureMax", data.getLong("temperatureMax"));
					//converted time to milliseconds
					dataPoints.put(i, new JSONArray().put(data.getLong("time") * 1000).put(data.getLong("temperatureMin")));
				}
				System.out.println(metricsJson.toString());
				//POST the json data via REST API
				try {
					KairosDBClient.getInstance().postNewData(metricsJson.toString(), "http://localhost:8888/api/v1/datapoints/");
				} catch (Exception e) {
					System.out.println("Error in measure tem active parser : " + e.getMessage());
					e.printStackTrace();
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}

	public void setPropertyName(String name) {
		// TODO Auto-generated method stub

	}

}
