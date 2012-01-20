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
		// TODO Return 0 for now
		return 0;
	}


	@Override
	public synchronized boolean next(LongWritable key, Text value) throws IOException {
		if (in == null || socket == null || socket.isClosed()) {
			this.close();
			return false;
		}
		
		key.set(0);
		value.clear();

		String line = in.readLine();
		value.set(line);
		
		return true;
	}
}