package com.harman.dbInsertion;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import com.harman.utils.ErrorType;

public class MariaModel {

	static MariaModel mariaModel;

	public static MariaModel getInstance() {
		if (mariaModel == null)
			mariaModel = new MariaModel();
		return mariaModel;
	}

	Connection connn = null;

	public Connection openConnection() {
		if (connn != null)
			return connn;
		try {
			Class.forName("org.mariadb.jdbc.Driver");
			// STEP 3: Open a connection
			System.out.println("Connecting to a selected database...");
			connn = DriverManager.getConnection("jdbc:mariadb://10.0.0.5/DEVICE_INFO_STORE", "root", ""); 
			System.out.println("Connected database successfully...");
		} catch (SQLException e) {
			System.out.println("Failed to connect db");
		} catch (Exception e) {
			System.out.println("Failed to connect db generic"+e.getMessage());
		}
		return connn;
	}

	public void closeConnection() {
		try {
			if (connn != null) {
				connn.close();
        connn=null;
			}
		} catch (SQLException se) {
			se.printStackTrace();
		}
	}

}
