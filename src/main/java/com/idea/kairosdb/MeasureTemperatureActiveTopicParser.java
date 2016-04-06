package com.idea.kairosdb;

import java.nio.charset.Charset;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.core.DataPointSet;

public class MeasureTemperatureActiveTopicParser implements TopicParserMetrics {
	private String m_metricName;
	MetricBuilder builder = MetricBuilder.getInstance();
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private long timestamp;
	
	public MetricBuilder parseTopic(String topic, JSONObject jsonObject) {
		if(jsonObject != null){
			try {
				m_metricName = topic;
				
				timestamp = jsonObject.getLong("timestamp") * 1000L;
				String value = jsonObject.getString("value");
				JSONObject currently = jsonObject.getJSONObject("currently");
				JSONObject daily = jsonObject.getJSONObject("daily");
				
				builder.addMetric(m_metricName)
				.addTag("latitude", jsonObject.getString("latitude"))
				.addTag("longitude", jsonObject.getString("longitude"))
				.addTag("timezone", jsonObject.getString("timezone"))
				.addTag("currently.time", currently.getString("time"))
				.addTag("currently.summary", currently.getString("summary"))
				//.add
				.addTag("currently.temperature", currently.getString("summary"))
				
				.addDataPoint(timestamp);
				
				
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		return builder;
	}



}
