package com.harman.batchJob;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

//import com.harman.dbinsertion.InsertIntoMongoDB;

// This a job which we would be scheduling in cscheduler
public class batchAnalyticJob implements Job
{
	
/*	public static batchAnalyticJob getInstance()
	{
		if (job_instance == null)
			job_instance = new batchAnalyticJob();
		return job_instance;
	}*/
	public batchAnalyticJob()
	{
		
	}
	
	/*public void setSparkContext(JavaSparkContext gContext)
	{
		this.spark_context = gContext;
		System.out.println(gContext.version());
		System.out.println("Spark context set \n");
		return;
	}*/

	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub
		//Mongo code that will be called automatically
		//InsertIntoMongoDB mongoInstance = InsertIntoMongoDB.getInstance();
		//mongoInstance.openConnection();
		//mongoInstance.ReadFromMongoDB();
		
		
		/*SparkSession spark = SparkSession.builder()
				.master("spark://10.0.0.4:7077")
				.appName("BatchAnalyticsApp")
				.config("spark.mongodb.input.uri", "mongodb://10.0.0.4/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.config("spark.mongodb.output.uri", "mongodb://10.0.0.4/DEVICE_INFO_STORE.SmartAudioAnalytics")
				.getOrCreate();

		// get Context
		JavaSparkContext global_context = new JavaSparkContext(spark.sparkContext());*/
		
		System.out.println("In execute method of Scheduler");
		JavaMongoRDD<Document> rdd = MongoSpark.load(SparkBatchJob.global_context);
		// Analyze data from MongoDB
		System.out.println(rdd.count());
		System.out.println(rdd.first().toJson());
		

	}
}	

