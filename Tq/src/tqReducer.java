package com.shkin.mr.tq;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class tqReducer extends Reducer<weather, IntWritable, Text, NullWritable>{

	@Override
	protected void reduce(weather arg0, Iterable<IntWritable> arg1,Context arg2)
		
			throws IOException, InterruptedException {
		
		int flag=0;
		for(IntWritable i : arg1){
			flag++;
			if(flag>2){
				break;
			}
			System.out.println(new Text(String.valueOf(arg1))+"分组后每年温度最高两天的温度"+flag);

			String msg=arg0.getYear()+"-"+arg0.getMonth()+"-"+arg0.getDay()+"----"+arg0.getWd();
			arg2.write(new Text(msg),NullWritable.get());
		}
		
	}
	

}
