package com.xzl.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author xzl
 * @create 2022-04-13 16:40
 */
public class TransReduceTest {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //这里的使用了之前自定义数据源小节中的ClickSource()
        env.addSource(new ClickSource()).map(new MapFunction<Event, Tuple2<String,Long>>() {

            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user,1L);
            }
        })
                .keyBy(r->r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 每到一条数据，用户pv的统计值加1
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }) .keyBy(r -> true) // 为每一条数据分配同一个key，将聚合结果发送到一条流中去
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 将累加器更新为当前最大的pv统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                .print();

        env.execute();

    }
}

