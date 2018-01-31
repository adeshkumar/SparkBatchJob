package com.harman.batchJob;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;
import org.bson.codecs.Decoder;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import static java.util.Collections.singletonList;
import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

//import com.harman.dbinsertion.InsertIntoMongoDB;

// This a job which we would be scheduling in cscheduler
public class batchAnalyticJob implements Job
{
	
	public batchAnalyticJob()
	{
		
	}
	
		
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException 
	{
				
		System.out.println("In execute method of Scheduler");
		JavaMongoRDD<Document> rdd = MongoSpark.load(SparkBatchJob.global_context);
		// Analyze data from MongoDB
		
		try{
			//JavaRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: {\"DeviceAnalytics.CriticalTemperatureShutDown\" : { $gte : 5 } } }")));
			JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: {\"date\" : { $gte : (new Date((new Date().getTime() - (30 * 24 * 60 * 60 * 1000)))) } } }").parse("{ $match: {\"DeviceAnalytics.CriticalTemperatureShutDown\" : { $gte : 5 } } }")));
			

			System.out.println(aggregatedRdd.first().toString());
			System.out.println(aggregatedRdd.count());
			//				InsertIntoMongoDB insertMongo = InsertIntoMongoDB.getInstance();
			//				insertMongo.openConnection();
//				insertMongo.updateCounter();
//				insertMongo.inserSingleRecordMongoDB(s);

//				InsertionIntoMariaDB insertMaria = InsertionIntoMariaDB.getInstance();
//				insertMaria.insertIntoMariaDB(s);
//
//				if (insertMongo.getCounter() >= count) {
//					if (insertMaria.getFeatureCounter() > emailAlertCounter) {
//						// send email
//						SparkTriggerThread.SendEmail("CriticalTemperatureShutDown",
//								insertMaria.getFeatureCounter());
//					}
//					insertMaria.resetFeatureCounter();
//				}

		}
		catch(Exception e)
		{
			
		}
	

		
		
		}

}	

