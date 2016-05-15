package com.idea.kafka.mqtt.bridge;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.idea.kairosdb.ConsumerThreadTemp;
import com.idea.kairosdb.KairosDBClient;
import com.idea.kairosdb.MongoDBParser;
import com.idea.kairosdb.MyTopicParser;
import com.idea.kairosdb.ActiveTemperatureParser;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


public class KafkaConsumer  {

	private final ConsumerConnector consumerConnector;
	private String topic;
	private ExecutorService m_executor;
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

	public KafkaConsumer(String zookeeper, String groupId, String topicName){
		//initialize the consumer connector
		this.topic = topicName;
		consumerConnector = this.initializeConsumerConnector(zookeeper, groupId);
		
	}

	/**
	 * To initialize the consumer connector, you need to define required properties. Properties are wrapped
	 * in ConsumerConfig object.
	 * ConsumerConnector is initialized using ConsumerConfig. On initializing ConsumerConnector, connection
	 * is established with Zookeeper. Zookeeper running on 'localhost:2181'
	 */
	private ConsumerConnector initializeConsumerConnector(String zookeeper, String groupId) {
		Properties props = new Properties();
		//zookeeper = "localhost:2181"
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.timeout.ms", "3000");
		props.put("auto.commit.interval.ms", "1000");
		ConsumerConfig conConfig = new ConsumerConfig(props);
		return Consumer.createJavaConsumerConnector(conConfig);
	}

	/**
	 * A Map topicCount is created with key as topic name and value as no. of threads. This map is used to create
	 * message streams by consumerConnector.
	 * ConsumerConnector returns the list of KafkaStream object for each topic wrapped in a map.
	 * You can fetch the list of Kafka stream from this map for a specific topic and then using 
	 * ConsumerIterator you can iterate the process messages
	 */
	public void consume() throws Exception{
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		//Key = topic name, Value = No. of thereads for topic
		topicCount.put(this.topic, new Integer(1));
		
		//ConsumerConnector creates the message stream for each topic
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector.createMessageStreams(topicCount);
		// Get Kafka stream for topic 
		
        List<KafkaStream<byte[], byte[]>> kStreamList = consumerStreams.get(topic);
        m_executor = Executors.newFixedThreadPool(1);
        int threadNumber = 0;
        // Iterate stream using ConsumerIterator
        for (final KafkaStream<byte[], byte[]> kStreams : kStreamList) {
            ConsumerThreadTemp ct = new ConsumerThreadTemp(topic, kStreams, threadNumber);
            if(topic.equals("mytopic")){
            	ct.setTopicParser(new MyTopicParser());
            }
            if(topic.equals("active-temperature")){
            	ct.setTopicParser(new ActiveTemperatureParser());
            }
            if(topic.equals("mongoDB")){
            	//consume all the mongoDB 
            	ct.setTopicParser(new MongoDBParser());
            }
            
            if(ct.getTopicParser() != null) {
            	//ct.run();
            	m_executor.submit(ct);
            	
            }
            threadNumber++;
        }
        //Shutdown the consumer connector
       // if (consumerConnector != null)   consumerConnector.shutdown();      
	}
	
	public static void start() throws InterruptedException {
		try {
			KafkaConsumer kafkaConsumer = new KafkaConsumer("localhost:2181", "testgroup", "mytopic");
			kafkaConsumer.consume();
			
			KafkaConsumer kafkaConsumer1 = new KafkaConsumer("localhost:2181", "testgroup", "active-temperature");
			kafkaConsumer1.consume();
			
			KafkaConsumer kafkaConsumer2 = new KafkaConsumer("localhost:2181", "testgroup", "mongoDB");
			kafkaConsumer2.consume();

		} catch (Exception e) {
			System.out.println("Error in KafkaConsumer start : " + e.getMessage());
		}
  }

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public static void main(String [] args){
		try {
			//KafkaConsumer kafkaConsumer = new KafkaConsumer("localhost:2181", "testgroup", "mytopic");
			//kafkaConsumer.consume();
			
			KafkaConsumer kafkaConsumer1 = new KafkaConsumer("localhost:2181", "testgroup", "active-temperature");
			kafkaConsumer1.consume();
			
			KafkaConsumer kafkaConsumer2 = new KafkaConsumer("localhost:2181", "testgroup", "mongoDB");
			kafkaConsumer2.consume();
			
		} catch (Exception e) {
			System.out.println("Exceptio in main  kafkaConsumer : " + e.getMessage());
		}

		
	}
}
