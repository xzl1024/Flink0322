package com.xzl.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xzl
 * @create 2022-03-22 23:48
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //有了自定义的source function ，调用addSource方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("SourceCustom");

        env.execute();


    }
}
