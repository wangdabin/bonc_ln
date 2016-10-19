package cn.com.bonc.hebei.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.com.bonc.shanxi.query.Utils;

public class TestFile extends Configured {

	enum RecordCounter {
		RIGHT_COUNTER, WRONG_COUNTER,
	}

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\\|");
			if (split.length >= 22) {
				String up = split[20];
				String down = split[21];
				long parseLong = Long.parseLong(up);
				long parseLong2 = Long.parseLong(down);
				long g = parseLong + parseLong2;
				long a = 2 * 1024 * 1024;
				if (split[1] != null && split[1] != "" && split[1] != "\\N"
						&& g >= a) {
					if (split.length > 31) {
						try {
							String URL = split[29];
							String TIME = split[17];
							String MDN = split[1];
							byte[] url = Utils.url2byte(URL);
							byte[] time = Utils.time2byte(TIME);
							byte[] mdn = Utils.mdn2byte(MDN);
						} catch (Exception e) {
							context.getCounter(RecordCounter.WRONG_COUNTER)
							.increment(1);
							context.write(value, new IntWritable());
						}
						context.getCounter(RecordCounter.RIGHT_COUNTER)
								.increment(1);
					}else{
						context.getCounter(RecordCounter.WRONG_COUNTER)
						.increment(1);
						context.write(value, new IntWritable());
					}
				}else{
					context.getCounter(RecordCounter.WRONG_COUNTER)
					.increment(1);
					context.write(value, new IntWritable());
				}
			}else{
				context.getCounter(RecordCounter.WRONG_COUNTER)
				.increment(1);
				context.write(value, new IntWritable());
			}
		};
		
	}

	
	public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable> { 
		protected void reduce(Text arg0, java.lang.Iterable<IntWritable> arg1, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context arg2) throws java.io.IOException ,InterruptedException {
			
		};
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job  = new Job(conf);
		job.setJobName("Test File ");
		job.setJarByClass(TestFile.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));   
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);    
	}
}
