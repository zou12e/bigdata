package com.flink;

import com.flink.out.SinkData;
import com.flink.out.StringData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogsData {

    public static void main(String[] args) throws Exception {


        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);

        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);

        DataStreamSource<String> dataStream = env.addSource(new SourceFromFile());

//        dataStream.print();

        dataStream.addSink(new StringData());
        env.execute("files logs");


    }

    public static class SourceFromFile extends RichSourceFunction<String> {
        private volatile Boolean isRunning = true;
//        private String files = "/data/logs/order-server/stdout.out";
        private String files = "/Users/jeff/Documents/ikjzd/vat/logs/order-server/debug.log";

        private String hit = "p6spy";
        private String regex = ".*Consume Time：(.*)\\sms.*";
        private Pattern r = Pattern.compile(regex);

        @Override
        public void run(SourceContext ctx) throws Exception {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(files));
            while (isRunning) {
                String line = bufferedReader.readLine();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                this.collect(line, bufferedReader, ctx);
            }
        }

        public void collect(String line, BufferedReader bufferedReader, SourceContext ctx) throws IOException {
            if (line.indexOf(hit) != -1) {
                System.out.println("line:" + line);
                Matcher m = r.matcher(line);
                if (m.find() && m.groupCount() > 0) {
                    long times = Long.valueOf(m.group(1));
                    if (times > 10) {
                        ctx.collect(" Consume Time：" + times + " ms |" + bufferedReader.readLine());
                    }
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


}
