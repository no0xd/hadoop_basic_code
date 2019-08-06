package com.netposa.mr3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class CombinerMapper extends Mapper<Object,Text,Text,LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = StringUtils.split(line);

        for(String word:split){
            context.write(new Text(word),new LongWritable(1L));
        }
    }
}
