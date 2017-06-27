package com.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TvBrandCount {

	public static class TvBrandMapper extends Mapper<LongWritable ,Text ,Text,IntWritable>{

		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
	/*  Mapper code to count Brand tv count*/

			String tvList=value.toString();
			String [] tv=tvList.split("\\|");
			
			if(tv[0].length()==2 || tv[1].equals("NA"))
			{
				//do nothing
			}
			else


			{
				
				con.write(new Text(tv[0]), new IntWritable(1));


			}}}
		
		public static class TvBrandReducer  extends Reducer<Text,IntWritable,Text,IntWritable>{

			
			public void reduce(Text key,Iterable<IntWritable>values,Context con) throws IOException, InterruptedException
			{ int sum=0;
				for(IntWritable val:values)
				{
					sum +=val.get();
				}
				con.write(key,new IntWritable( sum));
				
			}
		}


		public static void main(String[] args)throws Exception {
			// Driver code 
			
			Configuration conf=new Configuration();
			Job job=new Job(conf,"TVBrandCountS");
			job.setJarByClass(TVDriverClass.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			
			job.setMapperClass(TvBrandMapper.class);
			//job.setNumReduceTasks(0);
			job.setReducerClass(TvBrandReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);	
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);

		}

	}


