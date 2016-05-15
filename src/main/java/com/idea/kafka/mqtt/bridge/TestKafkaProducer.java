package com.idea.kafka.mqtt.bridge;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * MQTT Kafka Bridge
 * 
 * @author shazin
 *
 */
public class TestKafkaProducer {

	private static final String MQTT_BROKER_TOPICS = "mqttbrokertopics";
	private static final String MQTT_BROKER_PORT = "mqttbrokerport";
	private static final String MQTT_BROKER_HOST = "mqttbrokerhost";
	private static final String SERIALIZER_CLASS = "serializerclass";
	private static final String BROKER_LIST = "brokerlist";
	private static final String[] args = null;
	private static String[] type1 = {
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Aspen\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family W\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family TV\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch W\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room E\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room NW\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room SW\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family E\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Horse Picture\",\"state\":\"Green\"}",
			"{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
					+ (new Random().nextInt((50 - 5) + 1) + 5) + ",\"temperature\":75.43}",
			"{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
					+ (new Random().nextInt((50 - 5) + 10) + 5) + ",\"temperature\":75.43}"
//					,
//			"{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
//					+ (new Random().nextInt((50 - 5) + 15) + 5) + ",\"temperature\":75.43}" 
					};
	
	private static String[] type2 = {
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch E\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Aspen\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family W\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family TV\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family Couch W\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room E\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room NW \",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Round Room SW\",\"state\":\"Red\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Family E\",\"state\":\"Green\"}",
			"{\"client\":\"I.D.E.A. Lighting\",\"command\":\"Lighting State\",\"name\":\"Horse Picture\",\"state\":\"Red\"}",
			"{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
					+ (new Random().nextInt((50 - 5) + 1) + 5) + ",\"temperature\":75.43}",
			"{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
					+ (new Random().nextInt((50 - 5) + 1) + 5) + ",\"temperature\":75.43}",
			"{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
					+ (new Random().nextInt((50 - 5) + 1) + 5) + ",\"temperature\":75.43}", };
	private static ProducerConfig config;
	private static Producer<String, String> producer;

//	@Override
//	public void run() {
//		Options options = new Options();
//		Logger.getRootLogger().setLevel(Level.OFF);
//		options.addOption(BROKER_LIST, true, "Kafka Brokers List");
//		options.addOption(SERIALIZER_CLASS, true, "Kafka Serializer Class");
//		options.addOption(MQTT_BROKER_HOST, true, "MQTT Broker Host");
//		options.addOption(MQTT_BROKER_PORT, true, "MQTT Broker Port");
//		options.addOption(MQTT_BROKER_TOPICS, true, "MQTT Broker Topics");
//
//		CommandLineParser parser = new PosixParser();
//		CommandLine cmd;
//		try {
//			cmd = parser.parse(options, args);
//			Properties props = new Properties();
//			props.put("metadata.broker.list", cmd.getOptionValue(BROKER_LIST, "localhost:9092"));
//			props.put("serializer.class", cmd.getOptionValue(SERIALIZER_CLASS, "kafka.serializer.StringEncoder"));
//			config = new ProducerConfig(props);
//			producer = new Producer<String, String>(config);
//			String topicsArg = cmd.getOptionValue(MQTT_BROKER_TOPICS, "topic");
//			List<Topic> topicsList = new ArrayList<Topic>();
//			String[] topics = topicsArg.split(",");
//			boolean exit = false;
//			for (String topic : topics) {
//				topicsList.add(new Topic(topic, QoS.AT_LEAST_ONCE));
//			}
//			do {
//				printRed(type1, type2);
//			} while (!exit);
//
//			producer.close();
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}
//	}

	public static void start() {
		Options options = new Options();
		Logger.getRootLogger().setLevel(Level.OFF);
		options.addOption(BROKER_LIST, true, "Kafka Brokers List");
		options.addOption(SERIALIZER_CLASS, true, "Kafka Serializer Class");
		options.addOption(MQTT_BROKER_HOST, true, "MQTT Broker Host");
		options.addOption(MQTT_BROKER_PORT, true, "MQTT Broker Port");
		options.addOption(MQTT_BROKER_TOPICS, true, "MQTT Broker Topics");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
			Properties props = new Properties();
			props.put("metadata.broker.list", cmd.getOptionValue(BROKER_LIST, "localhost:9092"));
			props.put("serializer.class", cmd.getOptionValue(SERIALIZER_CLASS, "kafka.serializer.StringEncoder"));
			config = new ProducerConfig(props);
			producer = new Producer<String, String>(config);
			String topicsArg = cmd.getOptionValue(MQTT_BROKER_TOPICS, "sparkTopicTest");
			List<Topic> topicsList = new ArrayList<Topic>();
			String[] topics = topicsArg.split(",");
			boolean exit = false;
			for (String topic : topics) {
				topicsList.add(new Topic(topic, QoS.AT_LEAST_ONCE));
			}
			do {
				printRed(type1, type2);
			} while (!exit);

			producer.close();
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static void printRed(String[] type1, String[] type2) throws Exception {
		long startTimeRed = System.nanoTime();
		// System.out.println("Type1");
		int str1Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str2Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str3Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str4Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str5Random = new Random().nextInt((50 - 5) + 1) + 5;
		String str1 = "{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str1Random + ",\"temperature\":" + (str1Random + 33.8) + "}";
		String str2 = "{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ 50 + ",\"temperature\":" + 60 + "}";
		String str3 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str3Random + ",\"temperature\":" + (str3Random + 33.8) + "}";
		String str4 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:9f\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str4Random + ",\"temperature\":" + (str4Random + 33.8) + "}";
		String str5 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:8e\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str5Random + ",\"temperature\":" + (str5Random + 33.8) + "}";
		for (int i = 0; i < type1.length; i++) {

			JSONObject jobj = new JSONObject(type1[i]);
			// System.out.println(jobj.toString());

			Timestamp originalTimeStamp = new Timestamp(System.currentTimeMillis());
			Calendar calender = Calendar.getInstance();
			calender.setTimeInMillis(originalTimeStamp.getTime());
			jobj.put("TimeStamp", new Timestamp(calender.getTime().getTime()));
			KeyedMessage<String, String> kafkaMessage = new KeyedMessage<String, String>("sparkTopicTest", jobj.toString());
			producer.send(kafkaMessage);
			Thread.sleep(1000 * 1);
		}

		KeyedMessage<String, String> kafkaMessage1 = new KeyedMessage<String, String>("sparkTopicTest", str1);
		producer.send(kafkaMessage1);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage2 = new KeyedMessage<String, String>("sparkTopicTest", str2);
		producer.send(kafkaMessage2);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage3 = new KeyedMessage<String, String>("sparkTopicTest", str3);
		producer.send(kafkaMessage3);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage4 = new KeyedMessage<String, String>("sparkTopicTest", str4);
		producer.send(kafkaMessage4);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage5 = new KeyedMessage<String, String>("sparkTopicTest", str5);
		producer.send(kafkaMessage5);
		printGreen(type2, type1);
	}

	public static void printGreen(String[] type2, String[] type1) throws Exception {
		long startTimeGreen = System.nanoTime();
		// System.out.println("Type2");
		int str1Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str2Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str3Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str4Random = new Random().nextInt((50 - 5) + 1) + 5;
		int str5Random = new Random().nextInt((50 - 5) + 1) + 5;

		String str1 = "{\"deviceId\":\"28:db:b1:1f:06:00:00:d3\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str1Random + ",\"temperature\":" + (str1Random + 33.8) + "}";
		String str2 = "{\"deviceId\":\"28:26:1c:60:07:00:00:ad\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str2Random + ",\"temperature\":" + 60 + "}";
		String str3 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:c2\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str3Random + ",\"temperature\":" + (str3Random + 33.8) + "}";
		String str4 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:9f\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str4Random + ",\"temperature\":" + (str4Random + 33.8) + "}";
		String str5 = "{\"deviceId\":\"28:ff:2c:31:44:04:00:8e\",\"deviceType\":\"DS18B20\",\"celciusTemperature\":"
				+ str5Random + ",\"temperature\":" + (str5Random + 33.8) + "}";
		for (int i = 0; i < type2.length; i++) {

			// {"deviceId":"28:4d:4a:60:07:00:00:9f","deviceType":"DS18B20","celciusTemperature":11.19,"temperature":52.14,"TimeStamp":"2016-05-09
			// 19:26:40.953"}
			// {"deviceId":"28:26:1c:60:07:00:00:ad","deviceType":"DS18B20","celciusTemperature":15.69,"temperature":20.00,"TimeStamp":"2016-05-09
			// 19:26:41.063"}
			// {"deviceId":"28:db:b1:1f:06:00:00:d3","deviceType":"DS18B20","celciusTemperature":31.5,"temperature":88.7,"TimeStamp":"2016-05-09
			// 19:26:41.183"}
			// {"deviceId":"28:9c:23:60:07:00:00:8e","deviceType":"DS18B20","celciusTemperature":23.94,"temperature":75.09,"TimeStamp":"2016-05-09
			// 19:26:41.451"}
			// {"deviceId":"28:ff:2c:31:44:04:00:c2","deviceType":"DS18B20","celciusTemperature":18.94,"temperature":66.09,"TimeStamp":"2016-05-09
			// 19:26:41.743"}
			// {"deviceId":"192.168.86.35","deviceType":"GARAGE","openState":0,"TimeStamp":"2016-05-09
			// 19:26:42.295"}
			// {"deviceId":"192.168.86.35","deviceType":"GARAGE","openState":0,"TimeStamp":"2016-05-09
			// 19:26:44.864"}

			JSONObject jobj = new JSONObject(type2[i]);
			Timestamp originalTimeStamp = new Timestamp(System.currentTimeMillis());
			Calendar calender = Calendar.getInstance();
			calender.setTimeInMillis(originalTimeStamp.getTime());
			jobj.put("TimeStamp", new Timestamp(calender.getTime().getTime()));
			KeyedMessage<String, String> kafkaMessage = new KeyedMessage<String, String>("sparkTopicTest", jobj.toString());
			producer.send(kafkaMessage);
			Thread.sleep(1000 * 1);
		}
		KeyedMessage<String, String> kafkaMessage1 = new KeyedMessage<String, String>("sparkTopicTest", str1);
		producer.send(kafkaMessage1);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage2 = new KeyedMessage<String, String>("sparkTopicTest", str2);
		producer.send(kafkaMessage2);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage3 = new KeyedMessage<String, String>("sparkTopicTest", str3);
		producer.send(kafkaMessage3);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage4 = new KeyedMessage<String, String>("sparkTopicTest", str4);
		producer.send(kafkaMessage4);
		Thread.sleep(500);
		KeyedMessage<String, String> kafkaMessage5 = new KeyedMessage<String, String>("sparkTopicTest", str5);
		producer.send(kafkaMessage5);
		printRed(type1, type2);
	}

}
