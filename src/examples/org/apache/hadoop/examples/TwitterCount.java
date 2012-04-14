package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.buffer.impl.JOutputBuffer;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.stream.StreamInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is based on WordCount.java example
 * 
 * 
 * To run: bin/hadoop jar build/hadoop-examples.jar twittercount 
 *             [-s] [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
 */
public class TwitterCount extends Configured implements Tool {

  public static final Log LOG = LogFactory.getLog(TaskTracker.class);

  /**
   * Counts the words in each line. For each line of input, break the line into
   * words and emit them as (<b>word</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long seq;

    private boolean firstLine = false;

    public MapClass() {
      super();
      seq = 0;
    }

    public void blockForce(OutputCollector o) {
      JOutputBuffer jb = (JOutputBuffer) o;
      try {
        // jb.force();
        jb.stream(seq, true);
        seq++;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      
      while (itr.hasMoreTokens()){
    	  String tok = itr.nextToken();
    	  
    	  System.out.println("Tokenized:\t" + tok);
    	  LOG.info("TTokenized:\t" + tok);
    	  tok = filterWord(tok);
    	  System.out.println("Filtered:\t" + tok);
    	  LOG.info("Filtered:\t" + tok);
          
    	  if (tok != null){
    		  word.set(tok);
              
              output.collect(word, one);
              blockForce(output);
    	  }
      }
      
      /*
      // THIS ONLY GETS THE FIRST TOKEN
      String tok = itr.nextToken();
      word.set(Long.toString(seq));
      
      output.collect(word, one);
      blockForce(output);
      */
    }
    
    /**
     * Removes punctuation from the word, then finds the word's stem.
     * Then matches whether the word is a common word.
     * If so, return null
     * 
     * @param word
     * @return the filtered, stemmed word. null if word is common.
     */
    private String filterWord(String word){
    	WordFilter wf = WordFilter.getInstance();
    	WordStemmer ws = WordStemmer.getInstance();
    	
    	String temp = word.toLowerCase();
    	temp = WordFilter.filterPunctuation(word);
    	
    	temp = ws.getStem(temp);
    	if (wf.contains(temp))
    		return null;
    	else
    		return temp;
    }
    
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class Reduce extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {

    private Text word = new Text();

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      word.set(key.toString());
      output.collect(word, new IntWritable(sum));
    }
  }

  static int printUsage() {
    System.out
        .println("twittercount [-s interval] [-S <sort>] [-p <pipeline>] [-m maps] [-r reduces] [-w <window> <sliding_window>] input output");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /**
   * The main driver for word count map/reduce program. Invoke this method to
   * submit the map/reduce job.
   * 
   * @throws IOException
   *           When there is communication problems with the job tracker.
   */
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), TwitterCount.class);
    conf.setJobName("twittercount");

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    // conf.setInputFormat(TextInputFormat.class);
    conf.setNumReduceTasks(1);

    boolean sort = false;
    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-s".equals(args[i])) {
          conf.setFloat("mapred.snapshot.frequency", Float
              .parseFloat(args[++i]));
          conf.setBoolean("mapred.map.pipeline", true);
        } else if ("-p".equals(args[i])) {
          conf.setBoolean("mapred.map.pipeline", true);
        } else if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-S".equals(args[i])) {
          sort = true;
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else if ("-w".equals(args[i])) {
          System.out.println("Begin setting window properties");
          
          // not these
          conf.setBoolean("mapred.job.monitor", true); // or mapred.stream ??
          //conf.setBoolean("mapred.stream", true);
          
         
          // dirty hack
          conf.setBoolean("mapred.streaming.window", true);
          
          int windowsize = Integer.parseInt(args[++i]);
          conf.setInt("mapred.reduce.window", windowsize);
          int slidingtime = Integer.parseInt(args[++i]);
          conf.setInt("mapred.reduce.slidingtime", slidingtime);
          //conf.setBoolean("mapred.map.pipeline", true);
          
          System.out.println("Done setting all window properties -- "
              + windowsize + " " + slidingtime);
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: "
          + other_args.size() + " instead of 2.");
      return printUsage();
    }

    /*  ******* THESE ARE REQUIRED ******* */
    int num_mappers = other_args.get(0).split(";").length;
    conf.setInputFormat(StreamInputFormat.class);
    // This overrides the -m option, but is needed
    conf.setNumMapTasks(num_mappers);
    //conf.setBoolean("mapred.map.pipeline", true);

    // NOTE: StreamInputFormat
    StreamInputFormat.setInputStreams(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

    conf.setMapperClass(MapClass.class);
    if (sort) {
      conf.setReducerClass(IdentityReducer.class);
    } else {
      /* DO NOT USE A COMBINER */
      // conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);
    }

    System.out.println("Running TwitterCount on input: " + other_args.get(0));
    System.out.println("output: " + other_args.get(1));
    
    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TwitterCount(), args);
    System.exit(res);
  }

}
