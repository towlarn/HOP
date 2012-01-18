package org.apache.hadoop.mapred.lib.stream;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class StreamRecordReader<K, V> implements RecordReader<K, V> {

	public StreamRecordReader(JobConf job, StreamInputSplit split) {
		// TODO Auto-generated constructor stub
		
		// create connection to socket here, i.e. open the socket itself
		
		
	}

	
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public K createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public V createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(K key, V value) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
