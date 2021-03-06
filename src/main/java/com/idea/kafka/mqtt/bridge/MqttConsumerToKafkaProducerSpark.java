package com.idea.kafka.mqtt.bridge;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
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
public class MqttConsumerToKafkaProducerSpark {

	private static final String MQTT_BROKER_TOPICS = "mqttbrokertopics";
	private static final String MQTT_BROKER_PORT = "mqttbrokerport";
	private static final String MQTT_BROKER_HOST = "mqttbrokerhost";
	private static final String SERIALIZER_CLASS = "serializerclass";
	private static final String BROKER_LIST = "brokerlist";
	private static final String[] args = null;

	final static Logger logger = Logger.getLogger(MqttConsumerToKafkaProducer.class);
	
	public static void start() {
		Options options = new Options();
		
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
			ProducerConfig config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(config);

			MQTT mqtt = new MQTT();
			mqtt.setHost(cmd.getOptionValue(MQTT_BROKER_HOST, "whipple.dyndns-home.com"),
					Integer.parseInt(cmd.getOptionValue(MQTT_BROKER_PORT, "1883")));
			BlockingConnection connection = mqtt.blockingConnection();
			connection.connect();
			String topicsArg = cmd.getOptionValue(MQTT_BROKER_TOPICS, "topic");
			List<Topic> topicsList = new ArrayList<Topic>();
			String[] topics = topicsArg.split(",");
			for (String topic : topics) {
				topicsList.add(new Topic(topic, QoS.AT_LEAST_ONCE));
			}
			Topic[] mqttTopics = topicsList.toArray(new Topic[] {});
			byte[] qoses = connection.subscribe(mqttTopics);
			boolean exit = false;
			int sec = 0;
			do {
				JSONObject jobj;
				Message message = connection.receive();
				byte[] payload = message.getPayload();
				String strPayload = new String(payload);
				message.ack();

				/*
				 * Adding timestamp to the incoming messages by converting them
				 * to Json and appending new field at the end.
				 */

				try {
					jobj = new JSONObject(strPayload);
				} catch (JSONException e) {

					// Convert the expection in to JSON String and publish to
					// Kafka
					String str = "{\"JSONExceptoion\":\"" + strPayload + "\"}";
					jobj = new JSONObject(str);
				}

				Timestamp originalTimeStamp = new Timestamp(System.currentTimeMillis());
				Calendar calender = Calendar.getInstance();
				calender.setTimeInMillis(originalTimeStamp.getTime());
				calender.add(Calendar.SECOND, sec);
				jobj.put("TimeStamp", new Timestamp(calender.getTime().getTime()));
				sec = -5;
				/*
				 * Converting the incoming message from mqtt connection from
				 * "whipple.dyndns-home.com" to a kafka message and posting the
				 * message to "topic" topic on kafka.
				 */

				KeyedMessage<String, String> kafkaMessage = new KeyedMessage<String, String>(message.getTopic(),
						jobj.toString());
				producer.send(kafkaMessage);
			} while (!exit);
			connection.disconnect();
			producer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
