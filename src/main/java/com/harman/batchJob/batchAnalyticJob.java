package com.harman.batchJob;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.bson.Document;
import org.bson.codecs.Decoder;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.sql.Timestamp;

import com.harman.dbInsertion.InsertionIntoMariaDB;
import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

//import com.harman.dbinsertion.InsertIntoMongoDB;

// This a job which we would be scheduling in cscheduler
public class batchAnalyticJob implements Job, Serializable
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
	   implicitDS.createOrReplaceTempView("implicitDS");
	   System.out.println("Full Count="+implicitDS.count());
	   
	   Dataset<Row> dataOnTimeFilter = SparkBatchJob.global_spark_session.sql("select DeviceAnalytics.CriticalTemperatureShutDown from implicitDS where date > NOW() - INTERVAL 120 HOUR");
	   dataOnTimeFilter.createOrReplaceTempView("dataOnTimeFilter"); 
	   System.out.println("after time filter Count=" +dataOnTimeFilter.count()); 
	   dataOnTimeFilter.printSchema();
	   dataOnTimeFilter.show(1,false);
	   
	    
	    
	   /* Dataset<Row> element= dataOnTimeFilter.select("DeviceAnalytics.CriticalTemperatureShutDown","date").toDF();
	    element.createOrReplaceTempView("element");
	    element.show(3,false);*/
	    
	   
	  //  final Accumulator<Integer> totalShutDownCount = SparkBatchJob.global_context.accumulator(0);
	    Accumulator<Integer> accum =  SparkBatchJob.global_context.intAccumulator(0); 
	    System.out.println("Before foreach call");
	    
	    
	    
	    dataOnTimeFilter.foreach(new ForeachFunction<Row>(){

		
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row temp) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("[Shivam] Under foeach i should be in executor");
				int value =temp.getInt(0);
				System.out.println("[Shivam] value of criticaltempshutdown row is ="+value);
				 if(value > 4)
				 {
					accum.add(value);
				 }
	
				
			}
	    	
	    });
	 
	    
	    
	   /* Dataset<Row> elementTemp = SparkBatchJob.global_spark_session.sql("select DeviceAnalytics.CriticalTemperatureShutDown from element where DeviceAnalytics.CriticalTemperatureShutDown >= 4"
	    		+ " AND date > NOW() - INTERVAL 48 HOUR");
	    elementTemp.createOrReplaceTempView("elementTemp");
	    elementTemp.show(3,false);*/
	    
	    System.out.println("Count="+accum.value());
         
	   InsertionIntoMariaDB insertIntoMaria = InsertionIntoMariaDB.getInstance();
	    insertIntoMaria.putPerIntervalCriticalTempShutdown(accum.value());
	   
	    
		} catch (Exception e) {
			System.out.println("Exception in execute function of scheduler");
			e.printStackTrace();
			// TODO: handle exception
		}
	    
		
		

	}
}	

