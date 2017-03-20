package com.shkin.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

//import org.apache.hadoop.mapred.lib.HashPartitioner;

public class tqPartitaion extends HashPartitioner<weather, IntWritable >{
	
	@Override
	public int getPartition(weather key, IntWritable value, int numReduceTasks) {
		
		return (key.getYear()-1949)%numReduceTasks;
		//return super.getPartition(key, value, numReduceTasks);
	}
	

}
