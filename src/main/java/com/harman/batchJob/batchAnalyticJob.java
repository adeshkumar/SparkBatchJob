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
	private static JavaSparkContext spark_context = null;
	private static batchAnalyticJob job_instance = null;
	
	public static batchAnalyticJob getInstance()
	{
		if (job_instance == null)
			job_instance = new batchAnalyticJob();
		return job_instance;
	}
	public void setSparkContext(JavaSparkContext gContext)
	{
		this.spark_context = gContext;
		System.out.println("Spark context set \n");
		return;
	}

	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub
		//Mongo code that will be called automatically
		//InsertIntoMongoDB mongoInstance = InsertIntoMongoDB.getInstance();
		//mongoInstance.openConnection();
		//mongoInstance.ReadFromMongoDB();
		
		
		JavaMongoRDD<Document> rdd = MongoSpark.load(spark_context);
		// Analyze data from MongoDB
		System.out.println(rdd.count());
		System.out.println(rdd.first().toJson());

	}
}	

