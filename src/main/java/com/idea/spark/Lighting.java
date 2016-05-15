package com.idea.spark;

import java.sql.Timestamp;
import java.util.Calendar;

public class Lighting {

	private Timestamp timestamp;
	private Timestamp onTime;
	private Timestamp wasteTime;
	private String intialState;
	private String name;
	private String deviceID;
	
	

	public Lighting(Timestamp timestamp, Timestamp onTime, Timestamp wasteTime, String intialState, String name,
			String lightID) {
		super();
		this.timestamp = timestamp;
		this.onTime = onTime;
		this.wasteTime = wasteTime;
		this.intialState = intialState;
		this.name = name;
		this.deviceID = name.toLowerCase().replace(" ", "_");
	}

	public Lighting() {

	}
	
	public String getLightID() {
		return deviceID;
	}

	public void setLightID(String lightID) {
		this.deviceID = lightID;
	}

	public Timestamp getWasteTime() {
		return wasteTime;
	}

	public void setWasteTime(Timestamp wasteTime) {
		this.wasteTime = wasteTime;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Timestamp getOnTime() {
		return onTime;
	}

	public void setOnTime(Timestamp onTime) {
		this.onTime = onTime;
	}

	public String getIntialState() {
		return intialState;
	}

	public void setIntialState(String intialState) {
		this.intialState = intialState;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void updateState() {

	}

	public void updateOnTime() {

	}

}
