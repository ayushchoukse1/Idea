package com.idea.kairosdb;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.codehaus.jettison.json.JSONObject;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

public class ConsumerThread {

	public static final Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
	private final HttpClient m_kairosClient;
	
	private final String m_topic;
	private final KafkaStream<byte[], byte[]> m_stream;
	private final int m_threadNumber;
	//private TopicParser m_topicParser;
	private static final Charset UTF8 = Charset.forName("UTF-8");
	public ConsumerThread(HttpClient kairosClient, String topic, KafkaStream<byte[], byte[]> stream, int threadNumber)
	{
		m_kairosClient = kairosClient;
		m_topic = topic;
		m_stream = stream;
		m_threadNumber = threadNumber;
	}

	/*public void setTopicParser(TopicParser parser)
	{
		m_topicParser = parser;
	}*/

	public void run()
	{
		Thread.currentThread().setName(this.m_topic + "-" + this.m_threadNumber);
		logger.info("starting consumer thread " + this.m_topic + "-" + this.m_threadNumber);
		for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : m_stream)
		{
			try
			{
				logger.debug("message: " + messageAndMetadata.message());
				System.out.println("Consume: " + new String(messageAndMetadata.message(), UTF8));
				JSONObject jsonObject = new JSONObject(new String(messageAndMetadata.message()));
				MetricBuilder metric = MetricBuilder.getInstance();
				Response resp;
				long timestamp = System.currentTimeMillis();

				if(jsonObject.has("measure")){
					//Process the json object with StringTopicParse
					metric = new MeasureTemperaturePassiveTopicParser().parseTopic(m_topic, jsonObject);
				}else if(jsonObject.has("latitude") && jsonObject.has("offset")){
					//Ignore the message for now
					new MeasureTemperatureActiveTopicParser().parseTopic(m_topic, jsonObject);
				}else if(jsonObject.has("deviceId")){
					metric = MetricBuilder.getInstance();
					String deviceId = jsonObject.getString("deviceId");
					String metricName = deviceId.replace(":", "-");
					if("28:db:b1:1f:06:00:00:d3".equals(deviceId)){
						metric.addMetric(metricName)
							  .addTag("deviceId", metricName)
							  .addTag("deviceType", jsonObject.getString("deviceType"))
							  .addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
							  .addTag("temperature", jsonObject.getString("temperature"))
						      .addDataPoint(timestamp, jsonObject.getString("temperature"));
						
					}else if("28:4d:4a:60:07:00:00:9f".equals(deviceId)){
						//Message : {"deviceId":"28:4d:4a:60:07:00:00:9f","deviceType":"DS18B20","celciusTemperature":2.19,"temperature":35.94,"date":"2016-03-29T12:40:03.703Z"}
						metric.addMetric(metricName)
						  .addTag("deviceId", metricName)
						  .addTag("deviceType", jsonObject.getString("deviceType"))
						  .addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
						  .addTag("temperature", jsonObject.getString("temperature"))
					      .addDataPoint(timestamp, jsonObject.getString("temperature"));

					}else if("28:ff:2c:31:44:04:00:c2".equals(deviceId)){
						metric.addMetric(metricName)
						  .addTag("deviceId", metricName)
						  .addTag("deviceType", jsonObject.getString("deviceType"))
						  .addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
						  .addTag("temperature", jsonObject.getString("temperature"))
					      .addDataPoint(timestamp, jsonObject.getString("temperature"));
						
					}else if("28:26:1c:60:07:00:00:ad".equals(deviceId)){
						
						metric.addMetric(metricName)
						  .addTag("deviceId", metricName)
						  .addTag("deviceType", jsonObject.getString("deviceType"))
						  .addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
					      .addDataPoint(timestamp, jsonObject.getString("temperature"));
					}else{
						//No action
					}
				}else if(jsonObject.has("client") && jsonObject.getString("client").equals("I.D.E.A. Lighting")){
					/*
					 *  Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Family Couch E","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Horse Picture","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Aspen","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Family W","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Family Couch W","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Family TV","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Round Room E","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Round Room NW","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Round Room SW","state":"Green"}
						Message : {"client":"I.D.E.A. Lighting","command":"Lighting State","name":"Family E","state":"Green"}
					 */
					
					String metricName = jsonObject.getString("client");
					metricName = "IDEA Lighting";
					int state = 0;
					if(jsonObject.getString("state") != null){
						if(jsonObject.getString("state").equals("Green"))
							state = 1;
						else 
							state = 0;
					}
					metric.addMetric(metricName)
					  .addTag("client", metricName)
					  .addTag("command", jsonObject.getString("command"))
					  .addTag("name", jsonObject.getString("name"))
					  .addTag("state", jsonObject.getString("state"))
				      .addDataPoint(timestamp, state);

				}else if(jsonObject.has("manufacturer") && jsonObject.getString("manufacturer").equals("Honeywell") && jsonObject.getString("client").equals("IDEA Thermostat")){
						/*
						 * Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.temp","value":"70.0","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.OPERATING_MODE","value":"HEAT","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.FAN_MODE","value":"AUTO","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.override","value":"0","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.HOLD","value":"Disabled","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.SetPoint","value":"70.0","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.tstate","value":"0","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						Message : {"client":"IDEA Thermostat","measure":"sys.thermostat.FanState","value":"Off","timestamp":1459256714,"device":"Downstairs","manufacturer":"Honeywell"}
						 */
					String metricName = jsonObject.getString("client").replaceAll(".", "");
					metric.addMetric(metricName)
					  .addTag("client", metricName)
					  .addTag("measure", jsonObject.getString("measure"))
					  .addTag("value", jsonObject.getString("value"))
					  .addTag("device", jsonObject.getString("device"))					  
					  .addTag("manufacturer", jsonObject.getString("manufacturer"))
				      .addDataPoint(timestamp);

				}
				storeMetrics(metric);


/*				DataPointSet set = m_topicParser.parseTopic(m_topic, messageAndMetadata.key(),
						messageAndMetadata.message());
				for (DataPoint dataPoint : set.getDataPoints()){	
					//m_kairosClient.putDataPoint(set.getName(), set.getTags(), dataPoint);
				}
*/
			}
			/*catch (DatastoreException e)
			{
				// TODO: rewind to previous message to provide consistent consumption
				logger.error("Failed to store datapoints: ", e);
			}*/
			catch (Exception e)
			{
				logger.error("Failed to parse message: " + messageAndMetadata.message(), e.getMessage());
			}
		}
	}
	
	private void storeMetrics(MetricBuilder metric){
		try {
			if(metric != null){
				Response resp = m_kairosClient.pushMetrics(metric);
				System.out.println(resp.getStatusCode() + " --- " + resp.getErrors());
			}
		} catch (Exception e) {
			System.out.println("Error while storing data : " + e.getMessage());
		} 
		
	}

}
