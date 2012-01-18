package org.apache.hadoop.mapred.lib.stream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class StreamInputSplit implements InputSplit {
	
	//testing comment
	@Override
	public long getLength() throws IOException {
		// Return 0 for now, since it is an infinite stream.
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

}
