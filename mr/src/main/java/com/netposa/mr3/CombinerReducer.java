package com.netposa.mr3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0L;
        for(LongWritable value:values){
            count++;
        }
        context.write(key,new LongWritable(count));
    }
}
