package com.harman.batchJob;

import java.io.Serializable;

public class SparkJobTrigger implements Runnable, Serializable {

	private String feature;

	private SparkJobTrigger(String feature) {
		this.feature = feature;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void SendEmail(String feature, int value) {

		new Thread(new SparkJobTrigger(feature)).start();

	}

	@Override
	public void run() {
		System.out.println("****ALERT " + feature + " is need to be investigated !!!!!!!!!!!!!!!!!!");
	}

}
