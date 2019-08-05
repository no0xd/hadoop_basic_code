package com.netposa.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WCPartitioner extends Partitioner<Text,LongWritable>{
    public WCPartitioner() {
        super();
    }

    /**
     *
     * @param text
     * @param longWritable
     * @param i reducer的个数
     * @return
     */
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        if(text.getLength()>=5){
            return 0;
        }else{
            return 1;
        }
    }
}
