package com.flink.out;

import com.flink.model.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class StringData extends RichSinkFunction<String> {

    // 1,初始化
    @Override
    public void open(Configuration parameters) throws Exception
    {
        super.open(parameters);
    }

    // 2,执行
    @Override
    public void invoke(String str, Context context) throws Exception
    {
        System.out.println("str-------" + str);


    }

    // 3,关闭
    @Override
    public void close() throws Exception
    {

    }
}
