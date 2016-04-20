package com.idea.spark;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.idea.adapters.weather.forecastio.service.ForecastIOService;

import scala.Tuple2;

public final class BreakInputLines {

	static ProcessTempLines processTempLinesObject = new ProcessTempLines();
	static ProcessLightLines processLightLinesObject = new ProcessLightLines();
	static double forecastTemp = 0;

	public static void main(String[] argsold) throws Exception {
		MqttConsumerToKafkaProducer obj = new MqttConsumerToKafkaProducer();
		obj.start();
		ProcessUtility.fillLocator();
		String zkHosts = "localhost";
		String listenTopics = "topic";
		String listenerName = "testListener";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]")
				.set("spark.executor.memory", "1g");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(500));

		/*
		 * Setting the spark executor memory and local[2] are very important to
		 * avoid the following error: Initial job has not accepted any
		 * resources; check your cluster UI to ensure that workers are
		 * registered and have sufficient resources
		 * 
		 */

		int numThreads = 5;
		final AtomicLong dataCounter = new AtomicLong(0);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();

		String[] topics = listenTopics.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkHosts, listenerName,
				topicMap);
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		JavaDStream<String> lightLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("Lighting");
			}
		});

		JavaDStream<String> tempLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("temperature");
			}
		});

		// tempLines.print();
		readLightRDD(lightLines);
		//readTempRDD(tempLines);
		jssc.start();
		jssc.awaitTermination();
		// ProcessLightLines.readLightRDD(lightLines);

		// JavaDStream<String> lightsOnLines = lightLines.filter(new
		// Function<String, Boolean>() {
		// public Boolean call(String messages) {
		// return messages.contains("Green");
		// }

		// });

		// Timer timer = new Timer();
		// TimerTask hourlyTask = new TimerTask() {
		//
		// @Override
		// public void run() {
		// PersistData.persistLightData();
		// }
		// };
		// timer.schedule (hourlyTask, 0l, 1000*15);

		// jssc.start();
		// jssc.awaitTermination();

	}

	public static void readTempRDD(JavaDStream<String> dStream1) {

		System.out.println("Analyzing Temperature data");
		forecastTemp = processTempLinesObject.getForecastTemp();
		System.out.println("Forecasted Temp: " + forecastTemp);
		dStream1.foreachRDD(new Function<JavaRDD<String>, Void>() {
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {

					@Override
					public String call(String string) throws Exception {
						processTempLinesObject.readTempRDD(string);
						return string;
					}
				});

				/*
				 * //print objecHashmap for (Map.Entry<String, Lighting> entry :
				 * objectHashmap.entrySet()) { System.out.println(
				 * "Priting objectHashmap"); System.out.println(entry.getKey()+
				 * " : "+entry.getValue()); }
				 */
				List<String> ls = rowRDD.collect();
				ObjectMapper mapper = new ObjectMapper();
				/*
				 * for (int i = 0; i < ls.size(); i++) {
				 * 
				 * 
				 * Printing the RDD's as JSON Object
				 * 
				 * 
				 * Object json = mapper.readValue(ls.get(i), Object.class);
				 * mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
				 * //System.out.println(mapper.writerWithDefaultPrettyPrinter().
				 * writeValueAsString(json)); }
				 */
				return null;
			}
		});

	}
	
	public static void readLightRDD(JavaDStream<String> dStream) {


		dStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
				JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {
					/*
					 * Make modifications to the String here.
					 */
					@Override
					public String call(String string) throws Exception {
						processLightLinesObject.processLightString(string);
						return string;
					}
				});
				
				//print objecHashmap
				/*for (Map.Entry<String, Lighting> entry : objectHashmap.entrySet()) {
				    System.out.println(entry.getKey()+" : "+entry.getValue());
				}*/
				List<String> ls = rowRDD.collect();
				ObjectMapper mapper = new ObjectMapper();
				for (int i = 0; i < ls.size(); i++) {
					/*
					 * 
					 * Printing the RDD's as JSON Object
					 * 
					 */
					Object json = mapper.readValue(ls.get(i), Object.class);
					mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
					//System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
				}
				return null;
			}
		});
	}

}