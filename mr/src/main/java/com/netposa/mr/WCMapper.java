package com.netposa.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable,Text,Text,LongWritable>{


    public static Long one=1L;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key =======================>"+key);

        String[] words = StringUtils.split(value.toString());
        for(String word:words){
            context.write(new Text(word),new LongWritable(one));
        }
    }
}
