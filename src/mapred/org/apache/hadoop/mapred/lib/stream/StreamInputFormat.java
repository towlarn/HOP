package org.apache.hadoop.mapred.lib.stream;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class StreamInputFormat implements InputFormat<LongWritable, Text> {
	
	StreamInputSplit split;
	public static final String STREAM_SOURCES = "map.input.stream.sources";

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		
		if (!(split instanceof StreamInputSplit)) {
			throw new IOException("Wrong InputSplit format");
		}
		this.split = (StreamInputSplit) split;
	    //reporter.setStatus(split.toString());
	    return new StreamRecordReader(job, this.split);
	}

	@Override
	/**
	 * JobConf property for stream sources must be set in  STREAM_SOURCES
	 * JobConf stream source format:
	 *       IP1,port1;IP2,port2   etc
	 * e.g.  127.0.0.1,6000;127.0.0.1,7000 
	 */
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		
		String stream_source = job.get(STREAM_SOURCES);
		
		if (stream_source == null){
			throw new IOException("Stream source not specified");
		}
		
		String[] splits = stream_source.split(";");
		
		if (splits.length != numSplits){
			throw new IOException("Number of streaming sources does not equal number of splits");
		}
		
		
		StreamInputSplit[] split_arr = new StreamInputSplit[numSplits];
		int index = 0;
		for (String str : splits){
			String[] split2 = str.split(",");
			if (split2.length == 2){
				String addr = split2[0];
				try {
					int port = Integer.parseInt(split2[1]);
					split_arr[index] = new StreamInputSplit(addr, port);
				} catch (NumberFormatException e){
					e.printStackTrace();
					throw new IOException("Invalid port format");
				}
				
			}
			index++;
		}
		
		return split_arr;
	}

	public static void setInputStreams(JobConf conf, String stream_description) {
		conf.set(STREAM_SOURCES, stream_description);
	}
 
}
