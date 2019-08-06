package com.netposa.mr4;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowCount implements Writable {

    private Long upPackageCount=0L;
    private Long downPackageCount=0L;
    private Long upStreamCount=0L;
    private Long downStreamCount=0L;

    public FlowCount() {
    }

    public FlowCount(Long upPackageCount, Long downPackageCount, Long upStreamCount, Long downStreamCount) {
        this.upPackageCount = upPackageCount;
        this.downPackageCount = downPackageCount;
        this.upStreamCount = upStreamCount;
        this.downStreamCount = downStreamCount;
    }

    public Long getUpPackageCount() {
        return upPackageCount;
    }

    public void setUpPackageCount(Long upPackageCount) {
        this.upPackageCount = upPackageCount;
    }

    public Long getDownPackageCount() {
        return downPackageCount;
    }

    public void setDownPackageCount(Long downPackageCount) {
        this.downPackageCount = downPackageCount;
    }

    public Long getUpStreamCount() {
        return upStreamCount;
    }

    public void setUpStreamCount(Long upStreamCount) {
        this.upStreamCount = upStreamCount;
    }

    public Long getDownStreamCount() {
        return downStreamCount;
    }

    public void setDownStreamCount(Long downStreamCount) {
        this.downStreamCount = downStreamCount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upPackageCount);
        dataOutput.writeLong(downPackageCount);
        dataOutput.writeLong(upStreamCount);
        dataOutput.writeLong(downStreamCount);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upPackageCount=dataInput.readLong();
        this.downPackageCount=dataInput.readLong();
        this.upStreamCount=dataInput.readLong();
        this.downStreamCount=dataInput.readLong();
    }

    @Override
    public String toString() {
        return
                 upPackageCount
                +"\t"+ downPackageCount
                +"\t"+ upStreamCount
                +"\t"+ downStreamCount ;
    }

    public void sum(FlowCount other){
        this.upPackageCount+=other.getUpPackageCount();
        this.downPackageCount+=other.getDownPackageCount();
        this.upStreamCount+=other.getUpStreamCount();
        this.downStreamCount+=other.getDownStreamCount();
    }
}
