package com.harman.batchJob;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.harman.batchJob.SparkJobScheduler;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public final class SparkBatchJob {
	public static JavaSparkContext global_context;
	public static SparkSession global_spark_session;
	public static void main(final String[] args) throws InterruptedException {

		global_spark_session = SparkSession.builder()
				.master("spark://10.0.0.4:7077")
				.appName("BatchAnalyticsApp")
				.config("spark.mongodb.input.uri", "mongodb://10.0.0.4/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.config("spark.mongodb.output.uri", "mongodb://10.0.0.4/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.getOrCreate();

		// get Context
		 global_context = new JavaSparkContext(global_spark_session.sparkContext());
				
		SparkJobScheduler schedule = SparkJobScheduler.getInstance();
		schedule.mSparkJobScheduler();
		
		/*global_context.close();*/
	

  }
}