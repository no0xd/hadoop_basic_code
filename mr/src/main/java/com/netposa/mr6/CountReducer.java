package com.netposa.mr6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text,FlowCount,Text,FlowCount>{



    @Override
    protected void reduce(Text key, Iterable<FlowCount> values, Context context) throws IOException, InterruptedException {


        for(FlowCount flowCount:values){
            context.write(key,flowCount);
        }


    }

}
