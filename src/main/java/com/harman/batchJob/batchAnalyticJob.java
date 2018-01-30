package com.harman.batchJob;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
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
	
	public batchAnalyticJob()
	{
		
	}
	
		
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException 
	{
				
		System.out.println("In execute method of Scheduler");
		JavaMongoRDD<Document> rdd = MongoSpark.load(SparkBatchJob.global_context);
		// Analyze data from MongoDB
		
		rdd.foreach(new VoidFunction<Document>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Document s) throws Exception {
				System.out.println(s.toString());
				System.out.println(rdd.count());
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
	

		});
		
		}
}	

