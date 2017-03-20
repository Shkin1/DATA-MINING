package com.shkin.mr.tq;

import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.io.WritableComparator;

public class tqSort extends WritableComparator{

	public tqSort() {
		super(weather.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) { //因为weather实现了WritableComparable
		weather w1=(weather)a;     //父类转为子类
		weather w2=(weather)b;
		
		int c1=Integer.compare(w1.getYear(), w2.getYear());
		
		if(c1==0){
			return -Integer.compare(w1.getWd(), w2.getWd());
			
//			if(c2==0){
//				return -Integer.compare(w1.getWd(), w2.getWd());
//			}
//			
//			return c2;
		}
		
		
		return c1;
	}

	
	
	
}
