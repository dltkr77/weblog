package weblog;

import java.io.IOException;

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

public class TimebaseCounter extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Usage: TimebaseDetector <input dir> <output dir>");
			System.exit(-1);
		}
		int result = ToolRunner.run(new Configuration(), new TimebaseCounter(), args);
		System.exit(result);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "Timebase Detector");
		
		job.setJarByClass(TimebaseCounter.class);
		job.setMapperClass(TimebaseMapper.class);
		job.setReducerClass(TimebaseReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static class TimebaseMapper extends Mapper<LongWritable, Text, Text, Text> {
		/*
         * () == key
		 * Input : (offset), IP, Date, Time, URL, Parameter, Status
		 * Output : (IP, Date, Time), URL, Parameter, Status
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split("\\s+");
			String ip = words[0];
			String date = words[1];
			String time = words[2];
			String url = words[3];
			String param = words[4];
			String stat = words[5];
			
			context.write(new Text(ip + " " + date + " " + time), new Text(url + " " + param + " " + stat));
		}
	}
	
	public static class TimebaseReducer extends Reducer<Text, Text, Text, IntWritable> {
		/*
         * () == key
		 * Input : (IP, Date, Time), URL, Parameter, Status
		 * Output : (IP, Date, Time), count
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			int count = 0;
			for(Text value : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
		}
	}
}