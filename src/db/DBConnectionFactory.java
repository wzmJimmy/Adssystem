package db;

import db.mysql.MySQLConnection;

public class DBConnectionFactory {
	// This should change based on the pipeline.
		private static final String DEFAULT_DB = "mysql";
		
		public static DBConnection getConnection(String db) {
			switch (db) {
			case "mysql": return new MySQLConnection();
			case "mongodb": return null; 
			default: throw new IllegalArgumentException("Ivalid DB:"+db);
			}
		}
		
		public static DBConnection getConnection() {
			return getConnection(DEFAULT_DB);
			
		}

}
