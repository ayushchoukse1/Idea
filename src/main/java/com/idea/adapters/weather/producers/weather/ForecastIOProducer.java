package com.idea.adapters.weather.producers.weather;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import com.idea.adapters.weather.forecastio.service.ForecastIOService;
import com.idea.kafka.mqtt.bridge.KafkaProducer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ForecastIOProducer {
	// 192.168.184.135
	public static String getForecastIO(String address) throws Exception {
		ForecastIOService forecastIOObject = new ForecastIOService();
		return forecastIOObject.getWeatherForecast(address);
	}

	public static void start() throws Exception {

		String forecast = getForecastIO("1 Washington Sq, San Jose, CA 95192");
		System.out.println(forecast);
		KafkaProducer producer = new KafkaProducer();
		
		producer.initialize("active_temperature");
		producer.publishMessage(forecast);
		producer.closeConnection();
	}

}
