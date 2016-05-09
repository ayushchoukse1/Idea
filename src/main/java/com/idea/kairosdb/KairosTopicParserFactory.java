package com.idea.kairosdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.inject.Injector;

public class KairosTopicParserFactory implements TopicParserMetricsFactory {

	private Pattern m_classPattern = Pattern.compile("kairosdb\\.kafka\\.topicparser\\.(.*)\\.class");
	private Pattern m_topicsPattern = Pattern.compile("kairosdb\\.kafka\\.topicparser\\.(.*)\\.topics");
	Map<String, TPDefinition> m_definitionMap = new HashMap<String, TPDefinition>();
	private Injector m_injector;
	// Map of topic name to class name
	Map<String, TPDefinition> m_topicMap = new HashMap<String, TPDefinition>();

	public KairosTopicParserFactory(Properties properties){
		//m_injector = injector;

		for (String name : properties.stringPropertyNames()) {
			Matcher topicsMatcher = m_topicsPattern.matcher(name);
			if (topicsMatcher.matches()) {
				String propName = topicsMatcher.group(1);
				TPDefinition def = getDefinition(propName);

				for (String topic : Splitter.on(",").split(properties.getProperty(name))) {
					def.addTopics(topic);
				}

				continue;
			}

			Matcher classMatcher = m_classPattern.matcher(name);
			if (classMatcher.matches()) {
				String propName = classMatcher.group(1);
				TPDefinition def = getDefinition(propName);

				def.setClassName(properties.getProperty(name));
			}
		}

		//Put the class names into a map we can lookup by topic
		for (TPDefinition definition : m_definitionMap.values())
		{
			for (String topic : definition.getTopics())
			{
				m_topicMap.put(topic, definition);
			}
		}
	}
	
	private TPDefinition getDefinition(String name)
	{
		TPDefinition def = m_definitionMap.get(name);
		if (def == null)
		{
			def = new TPDefinition();
			def.setPropertyName(name);
			m_definitionMap.put(name, def);
		}

		return def;
	}
	
	// Return the TopicParser Class for a specific topic
	public TopicParserMetric getTopicParser(String topic) {
		TPDefinition def = m_topicMap.get(topic);
		String className = def.getClassName();

		Class<TopicParserMetric> aClass = null;
		try {
			aClass = (Class<TopicParserMetric>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		
		TopicParserMetric tp = m_injector.getInstance(aClass);
		tp.setPropertyName(def.getPropertyName());

		return tp;
	}

	// 
	public Set<String> getTopics() {
		return m_topicMap.keySet();
	}
	
	private static class TPDefinition
	{
		private List<String> m_topics;
		private String m_className;
		private String m_propertyName;

		public TPDefinition()
		{
			m_topics = new ArrayList<String>();
		}

		public void setPropertyName(String propertyName)
		{
			m_propertyName = propertyName;
		}

		public String getPropertyName()
		{
			return m_propertyName;
		}

		public void addTopics(String topic)
		{
			m_topics.add(topic);
		}

		public void setClassName(String className)
		{
			m_className = className;
		}

		public List<String> getTopics()
		{
			return m_topics;
		}

		public String getClassName()
		{
			return m_className;
		}
	}

}
