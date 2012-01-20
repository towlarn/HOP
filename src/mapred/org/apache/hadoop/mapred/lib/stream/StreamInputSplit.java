package org.apache.hadoop.mapred.lib.stream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.InputSplit;

public class StreamInputSplit implements InputSplit {
	
	private InetAddress addr;
	private int port;
	
	public StreamInputSplit(String addr, int port) throws IOException {
		this.addr = InetAddress.getByName(addr);
		this.port = port;
	}

	public InetAddress getAddr() {
		return addr;
	}

	public int getPort() {
		return port;
	}

	@Override
	public long getLength() throws IOException {
		// Return 0 for now, since it is an infinite stream.
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		// Each element of array is a <host,port> pair of incoming stream
		return null;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		addr = InetAddress.getByName(UTF8.readString(in));
		port = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		UTF8.writeString(out, addr.toString());
		out.writeInt(port);		
	}
	
	/*
	public static StreamInputSplit read(DataInput in) throws IOException {
		StreamInputSplit w = new StreamInputSplit();
		w.readFields(in);
		return w;
	}
	*/

}
