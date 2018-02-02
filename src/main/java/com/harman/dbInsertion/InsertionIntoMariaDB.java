package com.harman.dbInsertion;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import com.harman.utils.ErrorType;


public class InsertionIntoMariaDB {

	private InsertionIntoMariaDB() {

	}

	static InsertionIntoMariaDB isInsertionIntoMariaDB = null;

	public static InsertionIntoMariaDB getInstance() {
		if (isInsertionIntoMariaDB == null)
			isInsertionIntoMariaDB = new InsertionIntoMariaDB();
		return isInsertionIntoMariaDB;
	}

	//private int featureCounter = 0;

	public  void putPerIntervalCriticalTempShutdown(long count)
	{
		ErrorType response = ErrorType.NO_ERROR;
		MariaModel mariaModel = MariaModel.getInstance();
		Connection connection = mariaModel.openConnection();
		Statement stmt = null;
		try {
			stmt = connection.createStatement();
			try {

				int result = stmt.executeUpdate("INSERT INTO BatchCriticalTempShutDownTable (CriticalTempshutDown) VALUE(" + count + ")");
				if (result == 0)
					response = ErrorType.ERROR_INSERTING_DB;
				else{
					System.out.println("Critical Temperature shutdown count inserted to DB");
				}
			} catch (SQLException se) {
				response = ErrorType.ERROR_INSERTING_DB;
			}
		}
		catch (Exception e) {
			response = ErrorType.NETWORK_NOT_AVAILBLE;
		} 
		finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
			} catch (SQLException se) {
				response = ErrorType.ERROR_CLOSING_DB;
				System.out.println("SQLException while closing data");
			}
			
		}
		mariaModel.closeConnection();
		System.out.println("Response is : "+ response);
}
	
	/*public int getFeatureCounter() {
		return featureCounter;
	}

	public void updateFeatureCounter(int featureCounter) {
		featureCounter += featureCounter;
	}

	public void resetFeatureCounter() {
		featureCounter = 0;
	}*/

}