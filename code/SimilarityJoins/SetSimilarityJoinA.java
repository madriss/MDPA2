package edu.stanford.cs246.similarityjoin;


import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class SetSimilarityJoinA {
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new TextPreprocessing(), args);
	      
	      System.exit(res);
	   }

		public int run(String[] args) throws Exception {
			System.out.println(Arrays.toString(args));
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Set Similarity Naive");
		    job.setJarByClass(SetSimilarityJoinA.class);
		    job.setMapperClass(MainMapper.class);
		    job.setReducerClass(MainReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);																				

			FileInputFormat.addInputPath(job, new Path(args[0])); // here we enter the argument (input)
			FileOutputFormat.setOutputPath(job, new Path(args[1])); /// we get the output argument
			FileSystem fs = FileSystem.newInstance(getConf());

			if (fs.exists(new Path(args[1]))) { // if the output folder already exists, we delete it 
				fs.delete(new Path(args[1]), true);
			}

			job.waitForCompletion(true);
			return 0;
		  }

  public static class MainMapper
       extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text Key = new Text();
		private Text Value = new Text();
		
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// we perform the comparisons between lines
			Long Id = Long.parseLong(value.toString().split("\t")[0]);
			Value.set(value.toString().split("\t")[1]);
				
			for (Long id = 1L; id < Id; id++) {
				Key.set(Long.toString(id) + "|" + Long.toString(Id));
				context.write(Key, Value);
			}

			for (Long id = Id + 1L; id < 100 + 1L; id++) {//change here to select fewer lines
				Key.set(Long.toString(Id) + "|" + Long.toString(id));
				context.write(Key, Value);
			}
    }
  }

  public static class MainReducer
       extends Reducer<Text,Text,Text,FloatWritable> {
	   static enum CountersEnum { nb_comparisons }
		
		public static Float threshold = 0.8f;
		private FloatWritable Value = new FloatWritable();
		
    public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {
      
    		String doc1 = values.iterator().next().toString();
    		String doc2 = values.iterator().next().toString();
			
			
			Set<String> Set1 = new HashSet<String>(Arrays.asList(doc1.split("\\s+")));
			Set<String> Set2 = new HashSet<String>(Arrays.asList(doc2.split("\\s+")));
			Set<String> union = new HashSet<String>(Set1);
			Set<String> intersection = new HashSet<String>(Set2);
			union.addAll(Set2);
			intersection.retainAll(Set1);
			
			Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.nb_comparisons.toString());
			counter.increment(1);
			Float similarity = new Float(intersection.size()) / (new Float(union.size()));
		
		  if (similarity > threshold) {
				Value.set(similarity);
			  context.write(key, Value);
		  }
    }
		
  }


}
