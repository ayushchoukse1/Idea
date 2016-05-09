package com.idea.kairosdb;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.kairosdb.client.builder.MetricBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyTopicParser implements TopicParserMetric {

	private static final Logger LOG = LoggerFactory.getLogger(MyTopicParser.class);

	public void parseTopic(String topic, JSONObject jsonObject) {
		LOG.debug("In topic MyTopicParser : " + jsonObject);
		long timestamp = System.currentTimeMillis();
		MetricBuilder metric = MetricBuilder.getInstance();

		try {
			if (jsonObject.has("deviceId")) {
				metric = MetricBuilder.getInstance();
				String deviceId = jsonObject.getString("deviceId");
				String metricName = deviceId.replace(":", "-");
				if ("28:db:b1:1f:06:00:00:d3".equals(deviceId)) {
					metric.addMetric(metricName).addTag("deviceId", metricName)
							.addTag("deviceType", jsonObject.getString("deviceType"))
							.addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
							.addTag("temperature", jsonObject.getString("temperature"))
							.addDataPoint(timestamp, jsonObject.getString("temperature"));

				} else if ("28:4d:4a:60:07:00:00:9f".equals(deviceId)) {
					// Message :
					// {"deviceId":"28:4d:4a:60:07:00:00:9f","deviceType":"DS18B20","celciusTemperature":2.19,"temperature":35.94,"date":"2016-03-29T12:40:03.703Z"}
					metric.addMetric(metricName).addTag("deviceId", metricName)
							.addTag("deviceType", jsonObject.getString("deviceType"))
							.addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
							.addTag("temperature", jsonObject.getString("temperature"))
							.addDataPoint(timestamp, jsonObject.getString("temperature"));

				} else if ("28:ff:2c:31:44:04:00:c2".equals(deviceId)) {
					metric.addMetric(metricName).addTag("deviceId", metricName)
							.addTag("deviceType", jsonObject.getString("deviceType"))
							.addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
							.addTag("temperature", jsonObject.getString("temperature"))
							.addDataPoint(timestamp, jsonObject.getString("temperature"));

				} else if ("28:26:1c:60:07:00:00:ad".equals(deviceId)) {

					metric.addMetric(metricName).addTag("deviceId", metricName)
							.addTag("deviceType", jsonObject.getString("deviceType"))
							.addTag("celciusTemperature", jsonObject.getString("celciusTemperature"))
							.addDataPoint(timestamp, jsonObject.getString("temperature"));
				} else {
					// No action
				}
			} else if (jsonObject.has("client") && jsonObject.getString("client").equals("I.D.E.A. Lighting")) {
				/*
				 * Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Family Couch E","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Horse Picture","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Aspen","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Family W","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Family Couch W","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Family TV","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Round Room E","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Round Room NW","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Round Room SW","state":"Green"} Message :
				 * {"client":"I.D.E.A. Lighting","command":"Lighting State"
				 * ,"name":"Family E","state":"Green"}
				 */

				String metricName = jsonObject.getString("client");
				//metricName = "IDEA Lighting".concat(" ").concat(jsonObject.getString("name"));
				metricName = jsonObject.getString("name").replaceAll(" ", "_").replace(" ", "_").toLowerCase();
				
				int state = 0;
				if (jsonObject.getString("state") != null) {
					if (jsonObject.getString("state").equals("Green"))
						state = 1;
					else
						state = 0;
				}
				metric.addMetric(metricName).addTag("client", metricName).addTag("command", jsonObject.getString("command"))
						.addTag("name", jsonObject.getString("name")).addTag("state", jsonObject.getString("state"))
						.addDataPoint(timestamp, state);

			} else if (jsonObject.has("manufacturer") && jsonObject.getString("manufacturer").equals("Honeywell")
					&& jsonObject.getString("client").equals("IDEA Thermostat")) {
				/*
				 * Message : {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.temp","value":"70.0","timestamp":
				 * 1459256714,"device":"Downstairs","manufacturer": "Honeywell"}
				 * Message : {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.OPERATING_MODE","value":"HEAT",
				 * "timestamp":1459256714,"device":"Downstairs",
				 * "manufacturer":"Honeywell"} Message :
				 * {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.FAN_MODE","value":"AUTO","timestamp":
				 * 1459256714,"device":"Downstairs","manufacturer": "Honeywell"}
				 * Message : {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.override","value":"0","timestamp":
				 * 1459256714,"device":"Downstairs","manufacturer": "Honeywell"}
				 * Message : {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.HOLD","value":"Disabled","timestamp":
				 * 1459256714,"device":"Downstairs","manufacturer": "Honeywell"}
				 * Message : {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.SetPoint","value":"70.0","timestamp":
				 * 1459256714,"device":"Downstairs","manufacturer": "Honeywell"}
				 * Message : {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.tstate","value":"0","timestamp":
				 * 1459256714,"device":"Downstairs","manufacturer": "Honeywell"}
				 * Message : {"client":"IDEA Thermostat","measure":
				 * "sys.thermostat.FanState","value":"Off","timestamp":
				 * 1459256714,"device":"Downstairs","manufacturer": "Honeywell"}
				 */
				if(jsonObject.has("sys.thermostat.temp")){
					String metricName = "Thermostat-".concat(jsonObject.getString("device"));
					//System.out.println("Metric Name : " + metricName);
					timestamp = jsonObject.getLong("timestamp") * 1000L;
					double value = 0;
					if(isInteger(jsonObject.getString("value"))){
						value = jsonObject.getDouble("value");
					}

					metric.addMetric(metricName).addTag("client", metricName).addTag("measure", jsonObject.getString("measure"))
						.addTag("value", jsonObject.getString("value")).addTag("device", jsonObject.getString("device"))
						.addTag("manufacturer", jsonObject.getString("manufacturer")).addDataPoint(timestamp, value);
				}

			}
			if(metric != null)
				KairosDBClient.storeMetrics(metric);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean isInteger( String input )
	{
	   try
	   {
	      Integer.parseInt( input );
	      return true;
	   }
	   catch( Exception e)
	   {
	      return false;
	   }
	}
	public void setPropertyName(String name) {

	}

}
