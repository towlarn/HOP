package org.apache.hadoop.mapred.lib.stream;

import java.sql.*;

public class DataStreamer {
	
	private String connectionURL;
	private String query;

	public DataStreamer(String connectionURL, String query){
		this.connectionURL = connectionURL;
		this.query = query;
	}
	
	public void run(){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");

			con = DriverManager.getConnection (connectionURL);
			stmt = con.createStatement();
			rs = stmt.executeQuery(query);

			while (rs.next()){
				System.out.println("uid= " + rs.getString("uid") + " timestamp= " + rs.getString("timestamp") + 
						" text="+rs.getString("text"));
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			closeAll(rs, stmt, con);
		}
	}

	public static void closeAll(ResultSet rs, Statement stmt, Connection con) {
		if (rs != null) {
			try { rs.close(); } 
			catch (SQLException e) { e.printStackTrace(); }
		}
		if (stmt != null) {
			try { stmt.close(); } 
			catch (SQLException e) { e.printStackTrace(); }
		}
		if (con != null) {
			try { con.close(); } 
			catch (SQLException e) { e.printStackTrace(); }
		}
	}


	public static void main( String args[]) throws SQLException {
		String connectionURL = "jdbc:mysql://kozmo.cis.upenn.edu:3306/twitter02?user=towlarn&password=DU4YzhjM";
		String query = "select uid,timestamp,text from streaming_data ORDER BY timestamp asc limit 10";
		DataStreamer ds = new DataStreamer(connectionURL, query);
		
		ds.run();

	}
}
