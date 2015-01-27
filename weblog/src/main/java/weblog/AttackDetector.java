package weblog;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AttackDetector extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Usage: AttackDetector <input dir> <output dir>");
			System.exit(-1);
		}
		
		int result = ToolRunner.run(new Configuration(), new AttackDetector(), args);
		System.exit(result);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "Attack Detector");
		
		job.setJarByClass(AttackDetector.class);
		job.setMapperClass(AttackDetectorMapper.class);
		job.setReducerClass(AttackDetectorReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static class AttackDetectorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static String regex = 
				"(\\w+=\\w+)(\\;|%20|\'|\"|\\s+|[0-9]+)(and|or|exec|and|or|create|select)";
		private Pattern p = Pattern.compile(regex);
		/*
         * () == key
		 * Input : (offset), IP, Date, Time, URL, Parameter, Status
		 * Output : (IP, Date, Time, URL), 1[]
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String words[] = value.toString().split("\\s+");
			String ip = words[0];
			String date = words[1];
			String time = words[2];
			String url = words[3];
			String param = words[4];
			
			Matcher m = p.matcher(param);
			if(m.find() || param.contains("xp_cmdshell")) {
				context.write(new Text(ip + " " + date + " " + time + " " + url), new IntWritable(1));
			}
		}
	}
	
	public static class AttackDetectorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		/*
         * () == key
		 * Input : (IP, Date, Time, URL), Parameter, Status, 1[] -- 1 skip
		 * Output : (IP, Date, Time, URL), Probability
		 */
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
								
			int prob = 0;
			for(IntWritable value : values) {
				prob++;
			}
			context.write(key, new IntWritable(prob));
		}
	}
}