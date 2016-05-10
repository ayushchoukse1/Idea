package com.idea.kairosdb;

import java.nio.charset.Charset;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.core.DataPointSet;

public class MeasureTemperaturePassiveTopicParser implements TopicParserMetric {
	private String m_metricName;
	MetricBuilder builder = MetricBuilder.getInstance();
	private static final Charset UTF8 = Charset.forName("UTF-8");
	private long timestamp;
	
	public void parseTopic(String topic, JSONObject jsonObject) {
		if(jsonObject != null){
			try {
				m_metricName = jsonObject.getString("measure");
				timestamp = jsonObject.getLong("timestamp") * 1000L;
				String value = jsonObject.getString("value");

				builder.addMetric(m_metricName)
				.addTag("device", jsonObject.getString("device"))
				.addTag("value", jsonObject.getString("value"))
				.addTag("manufacturer", jsonObject.getString("manufacturer"))
				.addDataPoint(timestamp);
				
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		//return builder;
	}

	public void setPropertyName(String name) {
		// TODO Auto-generated method stub
		
	}



}
