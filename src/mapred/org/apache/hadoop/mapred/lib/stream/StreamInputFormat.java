package org.apache.hadoop.mapred.lib.stream;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class StreamInputFormat<K, V> implements InputFormat<K, V> {

	@Override
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
