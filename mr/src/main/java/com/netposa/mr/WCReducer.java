package com.netposa.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WCReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> iterator = values.iterator();
        int count=0;
        while(iterator.hasNext()){
            LongWritable countValue = iterator.next();
            count+=countValue.get();
        }
        context.write(key,new LongWritable(count));
    }
}
