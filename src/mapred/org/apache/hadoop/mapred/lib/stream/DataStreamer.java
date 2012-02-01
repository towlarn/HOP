package org.apache.hadoop.mapred.lib.stream;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DataStreamer implements Runnable {

	private String connectionURL;
	private String query;
	private String addr;
	private int port;
	
	private Socket socket;
	private PrintWriter out;
	
	private int id; // for debugging

	public DataStreamer(String connectionURL, String query, String addr, int port) throws IOException {
		this(connectionURL, query, addr, port, 0);
	}
	
	public DataStreamer(String connectionURL, String query, String addr, int port, int id) throws IOException{
		this.connectionURL = connectionURL;
		this.query = query;
		this.addr = addr;
		this.port = port;
		this.id = id;
		
		this.open();
	}
	
	private void open() throws IOException {
		InetAddress addr = InetAddress.getByName(this.addr);
		socket = new Socket(addr, port);
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
				out.println(test);
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


	public static void main(String args[]) throws SQLException, IOException {
		
		String connectionURL = "jdbc:mysql://kozmo.cis.upenn.edu:3306/twitter02?user=towlarn&password=DU4YzhjM";
		String query = "select uid,timestamp,text from streaming_data ORDER BY timestamp asc";

		if (args.length != 4) { printUsageAndQuit(); }

		int initial_offset = Integer.parseInt(args[0]);
		int num_threads = Integer.parseInt(args[1]);
		int num_rows_per_thread = Integer.parseInt(args[2]);
		String stream_source = args[3];
		
		query += " limit " + num_rows_per_thread; 

		/* Create Threadpool */
		int poolSize = 2;
		int maxPoolSize = 2;
		long keepAliveTime = 10;
		final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(5);

		ThreadPoolExecutor tp = new ThreadPoolExecutor(poolSize, maxPoolSize,
				keepAliveTime, TimeUnit.SECONDS, queue);

		
		String[] streams = parseStreams(stream_source, num_threads);
		
		for (int i = 0; i < num_threads; i++){
			String q = query + " offset " + (initial_offset + (i * num_rows_per_thread));
			System.out.println("executing query:  " + q);
			String[] split = streams[i].split(",");
			
			tp.execute(new DataStreamer(connectionURL, q, split[0], Integer.parseInt(split[1]), i));
		}
		
		tp.shutdown();
	}
	
	public static String[] parseStreams(String stream_source, int numStreams) throws IOException {
		String[] splits = stream_source.split(";");
		
		if (splits.length != numStreams){
			throw new IOException("Number of streaming sources does not equal number of splits");
		}
		
		// just check formatting
		for (String str : splits){
			String[] split2 = str.split(",");
			if (split2.length == 2){
				try {
					int port = Integer.parseInt(split2[1]);
				} catch (NumberFormatException e){
					e.printStackTrace();
					throw new IOException("Invalid port number");
				}
				
			} else {
				throw new IOException("Improperly formatted streaming sources. Usage: host1,port1;host2,port2;host3,port3 ...");
			}
		}
		return splits;
	}

	public static void printUsageAndQuit(){
		System.out.println("Usage: DataStreamer initial_offset num_threads num_rows_per_thread stream_source");
		System.out.println("stream_source format: host1,port1;host2,port2;host3,port3 ...");
		System.exit(0);
	}
}
