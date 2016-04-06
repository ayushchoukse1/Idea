package com.idea.kairosdb;

import org.codehaus.jettison.json.JSONObject;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.core.DataPointSet;

public interface TopicParserMetrics
{
	public MetricBuilder parseTopic(String topic, JSONObject jsonObject);
	//public void setPropertyName(String name);
}
