package db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


import db.DBConnection;
import entity.AdItem;
import entity.AdItem.AdItemBuilder;

public class MySQLConnection implements DBConnection {
	
	private Connection conn;
	public MySQLConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(MySQLDBUtil.URL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private boolean isNull(Connection conn) {
		if(conn == null) {System.err.println("DBconnection Failed!");return true;}
		return false;
	}


	@Override
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public List<AdItem> searchAdItems() {
		if(isNull(conn)) return new ArrayList<AdItem>();
		List<AdItem> adItems = new ArrayList<>();

		try {	
			String sql = "SELECT * from ad WHERE bid>0";
			PreparedStatement stmt = conn.prepareStatement(sql);
			
			ResultSet rSet = stmt.executeQuery(sql);
			AdItemBuilder builder = new AdItemBuilder();

			while (rSet.next()) {
				builder.setAd_id(rSet.getInt("ad_id"));
				builder.setBid(rSet.getFloat("bid"));
				builder.setImage_url(rSet.getString("image_url"));
				builder.setAdvertiser_id(rSet.getInt("advertiser_id"));
				builder.setAd_score(rSet.getFloat("ad_score"));
				adItems.add(builder.build());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return adItems;
	}

	@Override
	public float getBudget(int advertiser_id) {
		if(isNull(conn)) return -1;
		float budget = -1;
		try {	
			String sql = "SELECT budget from advertiser WHERE advertiser_id=?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setInt(1, advertiser_id);
			System.out.println(stmt.toString());
			
			ResultSet rSet = stmt.executeQuery();
			while (rSet.next()) {
				budget = rSet.getFloat("budget");	
			}
			System.out.println("curBudget" + budget);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return budget;
	}

	@Override
	public void updateBudget(int advertiser_id, double budget) {
		if(isNull(conn)) return;
		try {	
			String sql = "UPDATE advertiser SET budget=? WHERE advertiser_id=?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setDouble(1, budget);
			stmt.setInt(2, advertiser_id);
			System.out.println(stmt.toString());
			
			stmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("update budget done");
	}

	@Override
	public void updateBid(int ad_id, double bid) {
		if(isNull(conn)) return;
		try {	
			String sql = "UPDATE ad SET bid=? WHERE ad_id=?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setDouble(1, bid);
			stmt.setInt(2, ad_id);
			System.out.println(stmt.toString());
			
			stmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("update bid done");
	}

	@Override
	public long createAdvertiser(String advertiser_name, double budget) {
		if(isNull(conn)) return -1;
		
		//INSERT INTO advertiser (name,budget) VALUES ('apple',120);
		try {	
			String sql = "INSERT INTO advertiser (name,budget) VALUES (?, ?)";
			PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			stmt.setString(1, advertiser_name);
			stmt.setDouble(2, budget);
			System.out.println(stmt.toString());
			
	        //check the number of rows affected
	        if (stmt.executeUpdate() == 0) {
	            throw new SQLException("Creating advertiser failed, no rows affected.");
	        }

	        try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	System.out.println("Create advertiser done");
	            	return generatedKeys.getLong(1);//get the first auto-incremental id
	            }
	            else {
	                throw new SQLException("Creating advertiser failed, no ID obtained.");
	            }
	        }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;

	}

	@Override
	public long createAd(double bid, String image_url, int advertiser_id, double ad_score) {
		if(isNull(conn)) return -1;
		try {	
			String sql = "INSERT INTO ad (bid, image_url, advertiser_id, ad_score) VALUES (?, ?, ?, ?)";
			PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			stmt.setDouble(1, bid);
			stmt.setString(2, image_url);
			stmt.setInt(3, advertiser_id);
			stmt.setDouble(4, ad_score);
			System.out.println(stmt.toString());
			
	        //check the number of rows affected
	        if (stmt.executeUpdate() == 0) {
	            throw new SQLException("Creating ad failed, no rows affected.");
	        }

	        try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	System.out.println("Create ad done");
	            	return generatedKeys.getLong(1);//get the first auto-incremental id
	            }
	            else {
	                throw new SQLException("Creating ad failed, no ID obtained.");
	            }
	        }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1;
	}

}
