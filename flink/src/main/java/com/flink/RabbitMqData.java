package com.flink;

import com.flink.model.User;
import com.flink.out.SinkData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

public class RabbitMqData {

    public static void main(String[] args) throws Exception {


        // 1. 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. RabbitMQ配置
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(5672)
                .setUserName("guest")
                .setPassword("guest")
                .setVirtualHost("/")
                .build();

        // 3,添加资源
        DataStream<String> text = env.addSource(new RMQSource<String>(
                connectionConfig,
                "flink",
                true,
                new SimpleStringSchema()));

        DataStream<User> userDataStream = text
                .flatMap(new FlatMapFunction<String, User>() {
                    @Override
                    public void flatMap(String value, Collector<User> out) throws Exception {
                        System.out.println(value);
//                        User user = JSONObject.parseObject(value, User.class);
//                        out.collect(user);
                    }
                })
                .keyBy("name")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<User>() {
                    @Override
                    public User reduce(User a, User b) {
                        return new User(a.getName(), a.getAge() + b.getAge());
                    }
                })
                ;
        ;



        // 4,添加到流,去执行接收到的数据进行打印
//        userDataStream.print().setParallelism(1);
        userDataStream.addSink(new SinkData());


        // 5,执行工作,定义一个工作名称
        env.execute("rabbitmq flink ");

    }
}
