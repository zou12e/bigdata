package com.flink;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public static void main(String[] args) {

        String sql = "Consume Time：1442 ms 2020-05-28 14:20:01 |";

        String regex = ".*Consume Time：(.*)\\sms.*";



        // 创建 Pattern 对象
        Pattern r = Pattern.compile(regex);

        // 现在创建 matcher 对象
        Matcher m = r.matcher(sql);


        if ( m.find() && m.groupCount() > 0) {
            System.out.println(m.group(1));
        }



        System.out.println();



    }
}
