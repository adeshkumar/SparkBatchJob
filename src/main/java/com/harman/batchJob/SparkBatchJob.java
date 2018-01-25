package com.harman.batchJob;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public final class SparkBatchJob {

	public static void main(final String[] args) throws InterruptedException {

		SparkSession spark = SparkSession.builder()
				.master("spark://10.0.0.4:7077")
				.appName("BatchAnalyticsApp")
				.config("spark.mongodb.input.uri", "mongodb://10.0.0.4/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.config("spark.mongodb.output.uri", "mongodb://10.0.0./DEVICE_INFO_STORE.SmartAudioAnalytics")
				.getOrCreate();

		// get Context
		JavaSparkContext global_context = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> rdd = MongoSpark.load(global_context);

		// Analyze data from MongoDB
		System.out.println(rdd.count());
		System.out.println(rdd.first().toJson());
		global_context.close();


		//	  SparkConf conf = new SparkConf().setAppName("BatchAnalyticsApp").setMaster("spark://10.0.0.4:7077"); 
		//	  JavaSparkContext g_context = new JavaSparkContext(conf);


  }
}