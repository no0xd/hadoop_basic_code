package com.netposa.mr2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SoftPairWritable,NullWritable,SoftPairWritable,NullWritable>{

    //自定义计数器:使用枚举

    public static enum MyCounter{
        REDUCER_INPUT_KEY_RECOREDS,     //输入key统计
        REDUCER_INPUT_VALUE_RECOREDS    //输入value统计
    }

    @Override
    protected void reduce(SoftPairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.getCounter(MyCounter.REDUCER_INPUT_KEY_RECOREDS).increment(1L);

        for (NullWritable val:values){
            context.getCounter(MyCounter.REDUCER_INPUT_VALUE_RECOREDS).increment(1L);
            context.write(key,NullWritable.get());
        }
    }
}
