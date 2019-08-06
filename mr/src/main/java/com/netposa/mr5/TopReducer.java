package com.netposa.mr5;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopReducer extends Reducer<FlowKey,FlowWritable,NullWritable,FlowWritable>{

    @Override
    protected void reduce(FlowKey key, Iterable<FlowWritable> values, Context context) throws IOException, InterruptedException {

        for (FlowWritable flowWritable:values){
            context.write(NullWritable.get(),flowWritable);
        }
    }
}
