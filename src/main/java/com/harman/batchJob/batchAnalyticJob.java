package com.harman.batchJob;
import java.util.Date;
import java.util.Iterator;
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

import com.harman.dbInsertion.InsertionIntoMariaDB;
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
		try {
			
		
		// Load data and infer schema, disregard toDF() name as it returns Dataset
	   Dataset<Row> implicitDS = MongoSpark.load(SparkBatchJob.global_context).toDF();
	    implicitDS.printSchema();
	    implicitDS.show(1,false);
	    
	    Dataset<Row> element= implicitDS.select("DeviceAnalytics","date").toDF();
	    element.createOrReplaceTempView("element");
	    element.show(3,false);
	    
	    
	    Dataset<Row> elementTemp = SparkBatchJob.global_spark_session.sql("select DeviceAnalytics.CriticalTemperatureShutDown from element where DeviceAnalytics.CriticalTemperatureShutDown >= 4"
	    		+ "AND date > SUBDATE( CURRENT_DATE, INTERVAL 6 HOUR)");
	    elementTemp.createTempView("elementTemp");
	    elementTemp.persist();
	    elementTemp.show();
	    
	    System.out.println("Count="+elementTemp.count());
	    InsertionIntoMariaDB insertIntoMaria = InsertionIntoMariaDB.getInstance();
	    insertIntoMaria.putPerIntervalCriticalTempShutdown(elementTemp.count());
	    
	    /*Dataset<Row> elementTemp=  element.select("DeviceAnalytics.CriticalTemperatureShutDown");
	    elementTemp.createOrReplaceTempView("elementTemp");
	    elementTemp.show(3,false);*/
	   
	    
		} catch (Exception e) {
			System.out.println("Exception in execute function of scheduler");
			// TODO: handle exception
		}
	    
		
		
		
		}

}	

