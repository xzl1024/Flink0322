package com.xzl.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xzl
 * @create 2022-04-13 15:30
 */
public class TransFilterTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //传入匿名类实现FilterFunction
        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Mary");
            }
        });

        //传入FilterFunction 实现类
        stream.filter(new UserFilter()).print();

        env.execute();


    }

    private static class UserFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Mary");
        }
    }
}
