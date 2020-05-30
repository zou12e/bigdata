package com.flink;

import com.alibaba.flink.connectors.datahub.datastream.source.DatahubSourceFunction;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeHint;

public class DataHubData {

    // Endpoint以Region: 华东1为例，其他Region请按实际情况填写
    public static final String endPoint = "http://dh-cn-hangzhou.aliyuncs.com";
    public static final String accessId = "LTAI4GAxWMDSNtdgpAedwV4f";
    public static final String accessKey = "O7s9MpRUfxil86MPRbK4xvXUQ3nk2U";
    public static final String projectName = "test_jeff001";
    public static final String topicSourceName = "user";
    private static Long datahubStartInMs = 0L;//设置消费的启动位点对应的时间


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);
        env.setMaxParallelism(1);
        //set Source Function
        DatahubSourceFunction datahubSource =
                new DatahubSourceFunction(endPoint, projectName, topicSourceName, accessId, accessKey, datahubStartInMs,
                        Long.MAX_VALUE, 1, 1, 1);



        env.addSource(datahubSource)
                .flatMap(new FlatMapFunction<List<RecordEntry>, Tuple2<String,Long>>() {
                    public void flatMap(List<RecordEntry> ls, Collector<Tuple2<String,Long>> collector) throws Exception {
                        for(RecordEntry recordEntry : ls){
                            collector.collect(getTuple2(recordEntry));
                        }
                    }
                })
                .returns(new TypeHint<Tuple2<String,Long>>() {})
                .print();


        env.execute("datahub_demo");
    }

    private static Tuple2<String, Long> getTuple2(RecordEntry recordEntry) {
        Tuple2<String, Long> tuple2 = new Tuple2<String, Long>();
        TupleRecordData recordData = (TupleRecordData) (recordEntry.getRecordData());
        tuple2.f0 = (String) recordData.getField(0);
        tuple2.f1 = (Long) recordData.getField(1);

        return tuple2;
    }
}
