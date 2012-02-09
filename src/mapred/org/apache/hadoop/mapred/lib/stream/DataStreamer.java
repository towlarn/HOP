package org.apache.hadoop.mapred.lib.stream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;

public class DataStreamer implements Runnable {
	
	private static final String connectionURL = 
		"jdbc:mysql://kozmo.cis.upenn.edu:3306/twitter02?user=towlarn&password=DU4YzhjM";
	
	private static final String query = 
		"select uid,timestamp,text from streaming_data ORDER BY timestamp asc";
	
	
	private static int queries_executed = 0; 
	private static int initial_offset = 0;
	private static final Object query_offset_lock = new Object();

	private BlockingQueue<String> queue;
	private int rows;
	private boolean shutdown;
	
	private int current_id; // for debugging

	public DataStreamer(BlockingQueue<String> queue, int rows) throws IOException{
		shutdown = false;
		this.rows = rows; // rows per query
		this.queue = queue;
	}
	
	/***
	 * Gets and updates the static variable query_offset. Must be thread safe
	 */
	public int getQueryOffset(){
		synchronized (query_offset_lock){
			this.current_id = queries_executed;
			int offset = (initial_offset + (queries_executed * rows));
			queries_executed++;
			return offset;
		}
	}

	public void query(){
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");

			con = DriverManager.getConnection (connectionURL);
			stmt = con.createStatement();
			
			// form query
			int offset = getQueryOffset();
			String q = query + " limit " + this.rows + " offset " + offset;
			rs = stmt.executeQuery(q);

			if (rs.next()){
				do {
					//Timestamp x = rs.getTimestamp("timestamp");

					String str = rs.getString("uid") + " " + rs.getString("timestamp") + " " + rs.getString("text");
					//System.out.println(current_id + "(prod): "+ str);
					this.queue.put(str);
					
				} while (rs.next());
			}
			else {
				shutdown = true;
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

	@Override
	public void run() {
		while(!isShutdown()){
			query();
		}
	}
	
	public boolean isShutdown(){
		return shutdown;
	}
	
	/****** Static methods ******/
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

}
