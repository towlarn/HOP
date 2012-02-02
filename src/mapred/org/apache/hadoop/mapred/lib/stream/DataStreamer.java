package org.apache.hadoop.mapred.lib.stream;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataStreamer implements Runnable {

	private String connectionURL;
	private String query;
	
	private Socket socket;
	private PrintWriter out;
	
	private int id; // for debugging

	public DataStreamer(String connectionURL, String query, Socket socket) throws IOException {
		this(connectionURL, query, socket, 0);
	}
	
	public DataStreamer(String connectionURL, String query, Socket socket, int id) throws IOException{
		this.connectionURL = connectionURL;
		this.query = query;
		this.id = id;
		this.socket = socket;
		out = new PrintWriter(socket.getOutputStream(), true);		
	}
	
	public synchronized void close(){
		if (out != null){
			out.close();
			out = null;
		}
		if (socket != null){
			try { socket.close(); } 
			catch (IOException e) { e.printStackTrace(); }
			socket = null;
		}
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
				String test = ":  uid= " + rs.getString("uid") + " timestamp= " + rs.getString("timestamp") + 
					" text="+rs.getString("text");
				System.out.println(id + test);
				
				String str = rs.getString("uid") + " " + rs.getString("timestamp") + " " + rs.getString("text");
				out.println(str);
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
			close();
		}
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
