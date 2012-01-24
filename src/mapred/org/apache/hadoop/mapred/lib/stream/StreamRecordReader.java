package org.apache.hadoop.mapred.lib.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class StreamRecordReader implements RecordReader<LongWritable, Text> {

	private Socket socket;
	private BufferedReader in;
	private float progress;

	/**
	 * NOTE: Currently, the "Key" value has no meaning. It is always set as 0.
	 * Keys for FileSplits are the byte offset.
	 * 
	 * @param job
	 * @param split
	 * @throws IOException
	 */
	public StreamRecordReader(JobConf job, StreamInputSplit split) throws IOException {
		// create connection to socket here, i.e. open the socket itself
		InetAddress addr = split.getAddr();
		int port = split.getPort();
		socket = new Socket(addr, port);
		in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		progress = 0;
	}

	@Override
	public synchronized void close() throws IOException {
		if (in != null){
			in.close();
			in = null;
		}
		if (socket != null){
			socket.close();
			socket = null;
		}
		progress = 1;
	}

	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() {
		// TODO Return 0 for now
		return 0;
	}

	@Override
	public float getProgress() {
		return progress;
	}


	@Override
	/**
	 * This method MUST return false to signify end of map task
	 */
	public synchronized boolean next(LongWritable key, Text value) throws IOException {		
		if (in == null || socket == null || socket.isClosed()) {
			this.close();
			return false;
		}
		
		boolean end = false;
		
		key.set(0);
		value.clear();

		try {
			String line = in.readLine();
			if (line == null){
				line = "";
				end = true;
			}
			
			// testing
			if (line.equals("quit")){
				end = true;
				System.out.println("Closed the socket connection");			
				value.set(line);
			}
			// end
			
			value.set(line);
			
			System.out.println("Value was set as " + line);
			
		} catch (IOException e){
			// if the stream is closed
			end = true;
		}
		
		if (end){
			this.close();
			progress = 1;
		}
		
		return !end;
	}
}