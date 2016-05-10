package com.idea.spark;

public class WaterHeater {
	
	private static boolean state;

	public static boolean isState() {
		return state;
	}

	public static void setState(boolean state) {
		WaterHeater.state = state;
	} 
	
	

}
