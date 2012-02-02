package org.apache.hadoop.mapred.lib.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StreamerServer {

	private int port;
	private int offset;
	private int rows;
	private int SOCKET_TIMEOUT = 3 * 60 * 1000; // 3mins
	
	private String connectionURL = "jdbc:mysql://kozmo.cis.upenn.edu:3306/twitter02?user=towlarn&password=DU4YzhjM";
	private String query = "select uid,timestamp,text from streaming_data ORDER BY timestamp asc";
	
	private ThreadPoolExecutor tp;
	private ServerSocket ss;
	
	private boolean isShutdown;
	
	public StreamerServer(int port, int offset, int rows){
		this.port = port;
		this.offset = offset;
		this.rows = rows;
		
		isShutdown = false;
		
		this.query += " limit " + this.rows; 
		
		try {
			init();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void init() throws IOException{
		/* Create Threadpool */
		int poolSize = 5;
		int maxPoolSize = 5;
		long keepAliveTime = 10;
		final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(5);

		tp = new ThreadPoolExecutor(poolSize, maxPoolSize,
				keepAliveTime, TimeUnit.SECONDS, queue);

		ss = new ServerSocket(port);

		// thread used to receive commands
		tp.execute(new Runnable(){
			public void run() {
				BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
				String input;
				try {
					while((input = in.readLine()) != null){
						if (input.equals("shutdown")){
							isShutdown = true;
							ss.close();
							break;
						}
						// can also include ways to change initial_offset and num_rows_per_thread
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public void run(){
		int i = 0;
		System.out.println("Running Streamer Server...");
		try {
			while( !isShutDown() ){
				ss.setSoTimeout(SOCKET_TIMEOUT);
				Socket s = ss.accept();
				String q = query + " offset " + (offset + (i * rows));
				System.out.println("executing query:  " + q);
				tp.execute(new DataStreamer(connectionURL, q, s, i));
				i++;
			}
		}
		catch (SocketTimeoutException e){
			System.out.println("ServerSocket timed out");
		} catch (SocketException e) {
			//e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			this.shutdown();
			tp.shutdown();
		}
	}
	
	private void shutdown(){
		if (ss != null){
			try {
				ss.close();
			} catch (IOException e) {}
			ss = null;
		}
		System.out.println("Exit Streamer Server...");
	}

	public static void main(String args[]) throws SQLException, IOException {

		if (args.length != 3) { printUsageAndQuit(); }
		
		int port = Integer.parseInt(args[0]);
		int initial_offset = Integer.parseInt(args[1]);
		int num_rows_per_thread = Integer.parseInt(args[2]);
		
		StreamerServer ss = new StreamerServer(port, initial_offset, num_rows_per_thread);
		ss.run();
	}

	private boolean isShutDown() {
		return isShutdown;
	}


	public static void printUsageAndQuit(){
		System.out.println("Usage: DataStreamer  port  initial_offset  rows_per_thread");
		System.exit(0);
	}
}
