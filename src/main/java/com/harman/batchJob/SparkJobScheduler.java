package com.harman.batchJob;
import java.util.Date;

import org.apache.spark.api.java.JavaSparkContext;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;


public class SparkJobScheduler {
	
	private static SparkJobScheduler sjs = null;
	private static Scheduler batch_sched;
	
	public static SparkJobScheduler getInstance() {
		if (sjs == null)
			sjs = new SparkJobScheduler();
		return sjs;
	}
	
	public void mSparkJobScheduler(JavaSparkContext gContext)
	{
		try {
			/* create a scheduler */
			SchedulerFactory sf = new StdSchedulerFactory();
			batch_sched = sf.getScheduler("BatchAnalyticsScheduler");
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
		batchAnalyticJob new_job = batchAnalyticJob.getInstance();
		new_job.setSparkContext(gContext);
				
		JobDetail job1 = JobBuilder.newJob(batchAnalyticJob.class)
				.withIdentity("BatchAnalyticsJob", "group1")
				.build();

		//This trigger will run every minute in infinite loop

		Trigger trigger1 = TriggerBuilder.newTrigger()
				.withIdentity("everyMinuteTrigger", "group1")
				.startAt(new Date(System.currentTimeMillis()))
				.withSchedule( CronScheduleBuilder.cronSchedule( "0 0/1 * 1/1 * ? *"))
				.build();		
		try {
			Date ft = batch_sched.scheduleJob(job1, trigger1);
			batch_sched.start();
			System.out.println(job1.getKey() + " has been scheduled to run at: " + ft);
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

