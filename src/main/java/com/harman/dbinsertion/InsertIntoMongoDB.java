package com.harman.dbinsertion;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import org.bson.Document;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

public class InsertIntoMongoDB {

	Vector<String> listofJson = new Vector<String>();
	Object object = new Object();
	static InsertIntoMongoDB insertIntoMongoDB = null;

	private InsertIntoMongoDB() {

	}

	private long counter;

	public static InsertIntoMongoDB getInstance() {
		if (insertIntoMongoDB == null)
			insertIntoMongoDB = new InsertIntoMongoDB();
		return insertIntoMongoDB;
	}

	MongoClient mongoClient = null;

	/*
	 * Connection opens once only, and kept open till the Sparkclient runs
	 */
	public void openConnection() {
		if (mongoClient == null)
			mongoClient = new MongoClient("localhost", 27017);
	}

	public String  ReadFromMongoDB()
	{
		BasicDBObject allQuery = new BasicDBObject();
		BasicDBObject fields = new BasicDBObject();
		
		//MongoDatabase database = mongoClient.getDatabase("DEVICE_INFO_STORE");
		//MongoCollection<Document> col = database.getCollection("SmartAudioAnalytics");
		
		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB("DEVICE_INFO_STORE");
		DBCollection col = db.getCollection("SmartAudioAnalytics");
		allQuery.put("date", new BasicDBObject("$gte", (new Date((new Date().getTime() - (15 * 24 * 60 * 60 * 1000))))));
		
		//fields.put("AppAnalytics.SpeakerMode.Stereo", 1);
		//fields.put("harmanDevice.productName", 1);
		
		
		
		//fields.put("_id", 0);
		//fields.put("date", 1);
		DBCursor results = col.find(allQuery, fields);
	
		//new BasicDBObject("Stereo",new BasicDBObject("$gt", 3))
		while(results.hasNext()) {
		   		    
		    JSONObject output = new JSONObject(JSON.serialize(results.next()));
		    InsertionIntoMariaDB insertMaria = InsertionIntoMariaDB.getInstance();
			insertMaria.insertIntoMariaDB(output.toString());
		}
		results.close();
		return "succ";
	}
	public void inserSingleRecordMongoDB(String json) {
		try {
			System.out.println(json);
			Document document = Document.parse(json.toString());
			MongoDatabase database = mongoClient.getDatabase("DEVICE_INFO_STORE");
			MongoCollection<Document> table = database.getCollection("SmartAudioAnalytics");
			table.insertOne(document);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void inserIntoMongoDB(Vector<String> json) {
		System.out.println(json);
		List<Document> list = new ArrayList<>();
		for (String temp : json) {
			Document document = Document.parse(temp.toString());
			list.add(document);
		}
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("DEVICE_INFO_STORE");
		MongoCollection<Document> table = database.getCollection("SmartAudioAnalytics");
		table.insertMany(list);
		mongoClient.close();
	}

	public long getCounter() {
		return counter;
	}

	public void updateCounter() {
		++counter;
	}

}