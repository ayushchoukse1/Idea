package com.idea.kafka.mqtt.bridge;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	private final Producer<String, String> producer;
	private String m_topic;

	public void setTopic(String m_topic) {
		this.m_topic = m_topic;
	}

	public KafkaProducer(){
		ProducerConfig config = initializeProperties();
		producer = new Producer<String, String>(config);
	}
	
	/**
	 * To initialize the producer, you need to define required properties. Properties are wrapped in ProducerConfig object.
	 * Producer is initialized using ProducerConfig. On initializing Producer, connection is established between Producer and Kafka Broker. 
	 * Broker is running on 'localhost:9092'
	 */
	public ProducerConfig initializeProperties(){
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", "localhost:9092");
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		//producerProps.put("partitioner.class", "test.kafka.SimplePartitioner");
		//producerProps.put("partitioner.class", "com.idea.kairosdb.SimplePartitioner");
		producerProps.put("request.required.acks", "1");
		ProducerConfig producerConfig = new ProducerConfig(producerProps);
		return producerConfig;
	}
	
	/**
	 * In this method, string message is read from program passed as parameter.
	 * KeyedMessage is initialized with topic name and message. Now, producers send() API is used to send message by passing
	 * KeyedMessage reference. Based on topic name in KeyedMessage, producer sends message to corresponding topic.
	 * @param data
	 * @throws Exception
	 */
	public void publishMessage(String messagePayload) {
		try {
			KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(getTopic(), messagePayload);
			if(producer != null){
				producer.send(keyedMessage);
				//System.out.println(keyedMessage.toString());
			}
		} catch (Exception e) {
			System.out.println("Error in producer send : " + e.getMessage());
		}
	}

	public void closeConnection(){
		try {
			producer.close();
		} catch (Exception e) {

		}
	}

	public String getTopic() {
		return m_topic;
	}
	
}
