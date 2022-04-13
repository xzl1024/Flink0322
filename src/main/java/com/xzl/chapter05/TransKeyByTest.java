package com.xzl.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xzl
 * @create 2022-04-13 16:19
 */
public class TransKeyByTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //使用Lambda 表达式

//        KeyedStream<Event, String> keyedStream = stream.keyBy(event -> event.user);

        //使用匿名类实现KeySelector
        KeyedStream<Event, String> keyedStream1 = stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        });

        env.execute();
    }
}
