package com.flink;

import com.alibaba.flink.connectors.datahub.datastream.source.DatahubSourceFunction;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

public class DataHubData2 {

    // Endpoint以Region: 华东1为例，其他Region请按实际情况填写
    public static final String endPoint = "http://dh-cn-hangzhou.aliyuncs.com";
    public static final String accessId = "LTAI4GAxWMDSNtdgpAedwV4f";
    public static final String accessKey = "O7s9MpRUfxil86MPRbK4xvXUQ3nk2U";
    public static final String projectName = "test_jeff002";
    public static final String topicSourceName = "user";
    // private static Long datahubStartInMs = System.currentTimeMillis();//设置消费的启动位点对应的时间
    private static Long datahubStartInMs = 0L;//设置消费的启动位点对应的时间


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(2);
        env.setMaxParallelism(2);
        //set Source Function
        DatahubSourceFunction datahubSource =
                new DatahubSourceFunction(endPoint, projectName, topicSourceName, accessId, accessKey, datahubStartInMs,
                        Long.MAX_VALUE, 1, 1, 1);



        env.addSource(datahubSource)
                .flatMap(new FlatMapFunction<List<RecordEntry>, Tuple4<Long, String, Double, Long>>() {
                    public void flatMap(List<RecordEntry> ls, Collector<Tuple4<Long, String, Double, Long>> collector) throws Exception {
                        for(RecordEntry recordEntry : ls){
                            collector.collect(getTuple4(recordEntry));
                        }
                    }
                })
                .returns(new TypeHint<Tuple4<Long, String, Double, Long>>() {})
                .print();


        env.execute("datahub_demo");
    }

    private static Tuple4<Long, String, Double, Long> getTuple4(RecordEntry recordEntry) {
        Tuple4<Long, String, Double, Long> tuple2 = new Tuple4<Long, String, Double, Long>();
        TupleRecordData recordData = (TupleRecordData) (recordEntry.getRecordData());
        tuple2.f0 = (Long) recordData.getField(0);
        tuple2.f1 = (String) recordData.getField(1);
        tuple2.f2 = (Double) recordData.getField(2);
        tuple2.f3 = (Long) recordData.getField(3);
        return tuple2;
    }
}
