package com.flink;

import com.flink.model.User;
import com.flink.out.SinkData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaData {

    public static void main(String[] args) throws Exception {


        // 1. 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "sunday");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("sunday", new SimpleStringSchema(), properties));



        stream.print().setParallelism(1);

        // 5,执行工作,定义一个工作名称
        env.execute("kafka flink ");

    }
}
