package com.shkin.mr.tq;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobTq {
	
	public static void main(String[] args) {

		Configuration conf= new Configuration();
		
		conf.set("fs.defaultFS","hdfs://nodes1:8020");
		conf.set("yarn.resourcemanmager.hostname","nodes3");
		try {
		
		Job job = Job.getInstance(conf);

			job.setJarByClass(JobTq.class);
			
			job.setMapperClass(MyMapTq.class);
			job.setMapOutputKeyClass(weather.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setReducerClass(tqReducer.class);
			
			
			job.setPartitionerClass(tqPartitaion.class);
			job.setSortComparatorClass(tqSort.class);
			job.setGroupingComparatorClass(tqGroup.class);
			
			job.setNumReduceTasks(3);
			
			FileInputFormat.addInputPath(job, new Path("/tq/input/weathertwo.txt"));

			Path output=new Path("/tq/output");
			FileSystem fs = FileSystem.get(conf);
			
			if(fs.exists(output)){
				fs.delete(output, true);
			}
			FileOutputFormat.setOutputPath(job, output);
			
			boolean f = job.waitForCompletion(true);
			if(f){
				
				System.out.println("job执行完成");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}			
	}

}
