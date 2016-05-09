package com.idea.kairosdb;

import java.nio.charset.Charset;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.idea.kafka.mqtt.bridge.KafkaProducer;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ConsumerThreadTemp implements Runnable {
	public static final Logger LOG = LoggerFactory.getLogger(ConsumerThreadTemp.class);
	private static final Charset UTF8 = Charset.forName("UTF-8");

	private final String m_topic;
	private final KafkaStream<byte[], byte[]> m_stream;
	private final int m_threadNumber;
	private TopicParserMetric m_topicParser = null;
	
	public void setTopicParser(TopicParserMetric m_topicParser) {
		this.m_topicParser = m_topicParser;
	}

	public TopicParserMetric getTopicParser() {
		return m_topicParser;
	}

	public ConsumerThreadTemp(String topic, KafkaStream<byte[], byte[]> stream, int threadNumber){
		m_topic = topic;
		m_stream = stream;
		m_threadNumber = threadNumber;
	}
	
	public void run() {
		Thread.currentThread().setName(this.m_topic + "-" + this.m_threadNumber);
		//LOG.info("starting consumer thread " + this.m_topic + "-" + this.m_threadNumber);
		for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : m_stream){
			try{
				//LOG.debug("message: " + messageAndMetadata.message());
				JSONObject jsonObject = new JSONObject(new String(messageAndMetadata.message()));
				if(m_topicParser != null){
					m_topicParser.parseTopic(m_topic, jsonObject);
					//Produce to be consumed by the mongoDB consumer
					if(!this.m_topic.equals("mongoDB")){
		            	KafkaProducer producer = new KafkaProducer();
		        		producer.setTopic("mongoDB");
		        		producer.publishMessage(jsonObject.toString());
					}

				}

			}catch (Exception e){
					LOG.error("Failed to parse message: " + messageAndMetadata.message(), e.getMessage());
			}
		}
	}

}
