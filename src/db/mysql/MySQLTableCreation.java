package db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MySQLTableCreation {
	// Run this as Java application to reset db schema.
		public static void main(String[] args) {
			try {
				// Step 1 Connect to MySQL.
				System.out.println("Connecting to " + MySQLDBUtil.URL);
				Class.forName("com.mysql.cj.jdbc.Driver").getConstructor().newInstance();
				Connection conn = DriverManager.getConnection(MySQLDBUtil.URL);
				
				if (conn == null) {return;}
				Statement stmt = conn.createStatement();
				/*delete*/
				stmt.executeUpdate("DROP TABLE IF EXISTS ad");
				stmt.executeUpdate("DROP TABLE IF EXISTS advertiser");
				/*create*/
				stmt.executeUpdate("CREATE TABLE advertiser("
						+ "advertiser_id INT NOT NULL AUTO_INCREMENT,"
						+ "name VARCHAR(255),budget FLOAT,"
						+ "PRIMARY KEY(advertiser_id))");
				stmt.executeUpdate("CREATE TABLE ad("
						+ "ad_id INT NOT NULL AUTO_INCREMENT,"
						+ "bid FLOAT,image_url VARCHAR(2083),"
						+ "advertiser_id int NOT NULL,ad_score float,"
						+ "PRIMARY KEY(ad_id),"
						+ "FOREIGN KEY(advertiser_id) REFERENCES advertiser(advertiser_id))");
				
				System.out.println("Import done successfully");

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

}
