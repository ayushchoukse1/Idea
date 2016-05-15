package com.idea.kafka.mqtt.bridge;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import com.idea.spark.SparkProcess;

/**
 * MQTT Kafka Bridge
 * 
 * @author prashant
 *
 */
public class MqttConsumerToKafkaProducer {

	private static final String MQTT_BROKER_TOPICS = "mqttbrokertopics";
	private static final String MQTT_BROKER_PORT = "mqttbrokerport";
	private static final String MQTT_BROKER_HOST = "mqttbrokerhost";
	private static final String SERIALIZER_CLASS = "serializerclass";
	private static final String BROKER_LIST = "brokerlist";

	final static Logger logger = Logger.getLogger(MqttConsumerToKafkaProducer.class);
	public static void start() throws Exception {
		//Initialize producer
		System.out.println("Starting kafka producer");
		KafkaProducer kafkaProducer = new KafkaProducer();
		kafkaProducer.setTopic("mytopic");
		KafkaProducer kafkaProducerSpark = new KafkaProducer();
		kafkaProducerSpark.setTopic("sparktopic");
		
		MQTT mqtt = new MQTT();
		mqtt.setHost("whipple.dyndns-home.com", 1883);

		BlockingConnection connection = mqtt.blockingConnection();
		connection.connect();
		
		String topicsArg = "topic";
		List<Topic> topicsList = new ArrayList<Topic>();
		String[] topics = topicsArg.split(",");
		for(String topic:topics) {
			topicsList.add(new Topic(topic, QoS.AT_LEAST_ONCE));
		}

		Topic[] mqttTopics = topicsList.toArray(new Topic[]{});
		byte[] qoses = connection.subscribe(mqttTopics);

		
		boolean exit = false;
		
		while (!exit) {
			//JSONObject jobj;
			Message message = connection.receive();
			byte[] payload = message.getPayload();
			String strPayload = new String(payload);
			//System.out.println("Message : " + strPayload);
			//logger.info("mqtt : " + strPayload);
			// process the message then:
			message.ack();
//			try {
//				jobj = new JSONObject(strPayload);
//				System.out.println(strPayload);
//			} catch (JSONException e) {
//
//				// Convert the expection in to JSON String and publish to
//				// Kafka
//				String str = "{\"JSONExceptoion\":\"" + strPayload + "\"}";
//				jobj = new JSONObject(str);
//			}

			Timestamp originalTimeStamp = new Timestamp(System.currentTimeMillis());
			Calendar calender = Calendar.getInstance();
			calender.setTimeInMillis(originalTimeStamp.getTime());
			calender.add(Calendar.SECOND, -5);
			//jobj.put("TimeStamp", new Timestamp(calender.getTime().getTime()));
			//strPayload = jobj.toString();
			System.out.println("Test data: " + strPayload);
			//publish message
			kafkaProducer.publishMessage(strPayload);
			kafkaProducerSpark.publishMessage(strPayload);
			//SparkProcess.start();
		}

		//connection.disconnect();
		//kafkaProducer.closeConnection();
	}

}
