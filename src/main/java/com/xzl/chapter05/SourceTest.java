package com.xzl.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author xzl
 * @create 2022-03-22 23:05
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");


        //2.从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 3000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);
        //3.从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 3000L));

        //4.从Socket文本流读取
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);


//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");
//        stream3.print("3");
//        stream4.print("4");

        env.execute();


    }
}
