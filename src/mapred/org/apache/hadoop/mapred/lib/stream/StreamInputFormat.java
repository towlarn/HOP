package org.apache.hadoop.mapred.lib.stream;

import java.io.IOException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class StreamInputFormat<K, V> implements InputFormat<K, V> {
	
	StreamInputSplit split;

	@Override
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		if (!(split instanceof StreamInputSplit)) {
			throw new IOException("Wrong InputSplit format");
		}
		this.split = (StreamInputSplit) split;
	    //reporter.setStatus(split.toString());
	    return new StreamRecordReader(job, this.split);
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
