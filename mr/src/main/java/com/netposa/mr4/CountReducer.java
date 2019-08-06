package com.netposa.mr4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text,FlowCount,Text,FlowCount>{


    FlowCount initFlowCount=null;

    @Override
    protected void reduce(Text key, Iterable<FlowCount> values, Context context) throws IOException, InterruptedException {

        initFlowCount=new FlowCount();

        for(FlowCount flowCount:values){
            initFlowCount.sum(flowCount);
        }

        context.write(key,initFlowCount);

    }

}
