package edu.stanford.cs246.similarityjoin;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StopWords extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new StopWords(), args);
      
      System.exit(res);
   }

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "StopWords");
		job.setJarByClass(StopWords.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ","); // required for the CSV file export
		job.setNumReduceTasks(1);// here we choose the number of reducers

		FileInputFormat.addInputPath(job, new Path(args[0])); // here we enter the argument (input)
		FileOutputFormat.setOutputPath(job, new Path(args[1])); /// we get the output argument

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) { // if the output folder already exists, we delete it 
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends //Mapper code
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String token : value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")) {//here we split each word
				word.set(token); // we set each token to its lower case form in order to avoid words starting with an uppercase being different from lowercase words
				context.write(word, ONE);
			}
		}
	}

	public static class Reduce extends // Reducer code
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0; // count for each word
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum > 2500) { // condition for a stop word
				context.write(key, new IntWritable(sum)); // we write the stop word with its count
			}
		}
	}
}
