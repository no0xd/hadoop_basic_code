package com.netposa.mr2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SoftPairWritable,NullWritable,SoftPairWritable,NullWritable>{
    @Override
    protected void reduce(SoftPairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
