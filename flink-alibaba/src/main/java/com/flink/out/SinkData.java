package com.flink.out;

import com.flink.model.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SinkData extends RichSinkFunction<User> {

    // 1,初始化
    @Override
    public void open(Configuration parameters) throws Exception
    {
        super.open(parameters);
    }

    // 2,执行
    @Override
    public void invoke(User user, Context context) throws Exception
    {
        System.out.println("value.toString()-------" + user);

//        User user = JSONObject.parseObject(value, User.class);
//
//        System.out.println("user.toString()-------" + user.toString());
    }

    // 3,关闭
    @Override
    public void close() throws Exception
    {

    }
}
