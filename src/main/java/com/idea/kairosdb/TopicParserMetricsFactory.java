package com.idea.kairosdb;

import java.util.Set;

public interface TopicParserMetricsFactory {
	public TopicParserMetric getTopicParser(String topic);
	public Set<String> getTopics();
}
