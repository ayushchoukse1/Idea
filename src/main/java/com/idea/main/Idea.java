package com.idea.main;

import com.idea.adapters.weather.producers.weather.ForecastIOProducer;
import com.idea.kafka.mqtt.bridge.KafkaConsumer;
import com.idea.kafka.mqtt.bridge.MqttConsumerToKafkaProducer;
import com.idea.kafka.mqtt.bridge.MqttConsumerToKafkaProducerSpark;
import com.idea.kafka.mqtt.bridge.TestKafkaProducer;
import com.idea.spark.SparkProcess;

public class Idea {
	public static void main(String[] args){
		//Start the mqtt listener
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					MqttConsumerToKafkaProducer.start();
				} catch (Exception e) {
					System.out.println("Error in MQttConsumerToKafkaProducer : " + e.getMessage());
				}
			}
		});

		//start the kafka collector
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					KafkaConsumer.start();
				} catch (Exception e) {
					System.out.println("Error in KafkaConsumer : " + e.getMessage());
				}
			}
		});
		
		//start the active weather data collector
		Thread t3 = new Thread(new Runnable() {
			public void run() {
				try {
					ForecastIOProducer.start();
				} catch (Exception e) {
					System.out.println("Error in ForecastIOProducer : " + e.getMessage());
				}
			}
		});
		Thread t4 = new Thread(new Runnable() {
			public void run() {
				try {
					TestKafkaProducer.start();
				} catch (Exception e) {
					System.out.println("Error in TestKafkaProducer : " + e.getMessage());
				}
			}
		});
		Thread t5 = new Thread(new Runnable() {
			public void run() {
				try {
					MqttConsumerToKafkaProducerSpark.start();
				} catch (Exception e) {
					System.out.println("Error in MQttConsumerToKafkaProducerSpark : " + e.getMessage());
				}
			}
		});
		
		Thread t6 = new Thread(new Runnable() {
			public void run() {
				try {
					SparkProcess.start();
				} catch (Exception e) {
					System.out.println("Error in SparkProcess : " + e.getMessage());
				}
			}
		});
		
		//start the active weather data collector
		t4.start();

		
		//start the active weather data collector
		t6.start();			

	}
}
