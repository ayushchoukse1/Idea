package com.idea.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.idea.kafka.mqtt.bridge.TestKafkaProducer;

import scala.Tuple2;

public final class SparkProcess {

	static ProcessTempLines processTempLinesObject = new ProcessTempLines();
	static ProcessLightLines processLightLinesObject = new ProcessLightLines();
	static double forecastTemp = 0;

	public static void start() {

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

		// Separate Light lines from all the input messages
		JavaDStream<String> lightLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("Lighting");
			}
		});

		// Separate Temperature lines from all the input messages
		JavaDStream<String> tempLines = lines.filter(new Function<String, Boolean>() {
			public Boolean call(String messages) {
				return messages.contains("temperature");
			}
		});

		//readTempRDD(tempLines);
		readLightRDD(lightLines);

		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		exec.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				System.out.println("TEST : persistLightData Running every 15 seconds");
				PersistData.persistLightData();
			}
		}, 5, 15, TimeUnit.SECONDS);

		jssc.start();
		jssc.awaitTermination();
	}

	public static void readTempRDD(JavaDStream<String> dStream1) {
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
				
				List<String> ls = rowRDD.collect();
				ObjectMapper mapper = new ObjectMapper();
				return null;
			}
		});
	}

	public static void readLightRDD(JavaDStream<String> dStream) {
		ExternalData.setSunTime();
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
				List<String> ls = rowRDD.collect();
				ObjectMapper mapper = new ObjectMapper();
				return null;
			}
		});
	}
}