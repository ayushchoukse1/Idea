package com.idea.kafka.mqtt.bridge;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.idea.kairosdb.ConsumerThread;
import com.idea.kairosdb.KairosDBClient;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class KafkaConsumer {

	private ConsumerConnector consumerConnector = null;
	private String topic = "mytopic";
	
	
	/**
	 * To initialize the consumer connector, you need to define required properties. Properties are wrapped
	 * in ConsumerConfig object.
	 * ConsumerConnector is initialized using ConsumerConfig. On initializing ConsumerConnector, connection
	 * is established with Zookeeper. Zookeeper running on 'localhost:2181'
	 */
	public void initialize(String topic){
		setTopic(topic);
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "testgroup");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.timeout.ms", "300");
		props.put("auto.commit.interval.ms", "1000");
		ConsumerConfig conConfig = new ConsumerConfig(props);
		consumerConnector = Consumer.createJavaConsumerConnector(conConfig);
	}

	/**
	 * A Map topicCount is created with key as topic name and value as no. of threads. This map is used to create
	 * message streams by consumerConnector.
	 * ConsumerConnector returns the list of KafkaStream object for each topic wrapped in a map.
	 * You can fetch the list of Kafka stream from this map for a specific topic and then using 
	 * ConsumerIterator you can iterate the process messages
	 */
	public void consume(){
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		//Key = topic name, Value = No. of thereads for topic
		topicCount.put(topic, new Integer(1));
		//ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector.createMessageStreams(topicCount);
		// Get Kafka stream for topic 'mytopic'
        List<KafkaStream<byte[], byte[]>> kStreamList =
                                             consumerStreams.get(topic);
        int threadNumber = 0;
        // Iterate stream using ConsumerIterator
        for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
            ConsumerIterator<byte[], byte[]> consumerIte = kStreams.iterator();
            ConsumerThread ct = new ConsumerThread(KairosDBClient.getInstance(), topic, kStreams, threadNumber);
            ct.run();
            //Properties props = new Properties();
            //props.put(key, value)
            //ct.setTopicParser(new StringTopicParser());
			/*while (consumerIte.hasNext()) {
				System.out.println("Message consumed from topic[" + topic + "] : " + new String(consumerIte.next().message()));
				//if this message contains a keyword then load a specific Topic Parser and process it further.
				
			}*/
            threadNumber++;
        }
        //Shutdown the consumer connector
        if (consumerConnector != null)   consumerConnector.shutdown();      
	}
	
	public static void start() throws InterruptedException {
        KafkaConsumer kafkaConsumer = new KafkaConsumer();
        // Configure Kafka consumer
        kafkaConsumer.initialize("mytopic");
        // Start consumption
        kafkaConsumer.consume();
  }

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
