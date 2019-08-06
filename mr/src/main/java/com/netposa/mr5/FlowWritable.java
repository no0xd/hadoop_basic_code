package com.netposa.mr5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowWritable implements Writable {


    private String phoneNum;
    private Long upPackageCount;
    private Long downPackageCount;
    private Long upStreamCount;
    private Long downStreamCount;

    public FlowWritable() {

    }

    public FlowWritable(String phoneNum, Long upPackageCount, Long downPackageCount, Long upStreamCount, Long downStreamCount) {
        this.phoneNum = phoneNum;
        this.upPackageCount = upPackageCount;
        this.downPackageCount = downPackageCount;
        this.upStreamCount = upStreamCount;
        this.downStreamCount = downStreamCount;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
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
        dataOutput.writeUTF(phoneNum);
        dataOutput.writeLong(upPackageCount);
        dataOutput.writeLong(downPackageCount);
        dataOutput.writeLong(upStreamCount);
        dataOutput.writeLong(downStreamCount);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNum=dataInput.readUTF();
        this.upPackageCount=dataInput.readLong();
        this.downPackageCount=dataInput.readLong();
        this.upStreamCount=dataInput.readLong();
        this.downStreamCount=dataInput.readLong();
    }

    @Override
    public String toString() {
        return
                phoneNum
                +"\t"+ upPackageCount
                +"\t"+ downPackageCount
                +"\t"+ upStreamCount
                +"\t"+ downStreamCount;
    }

}
