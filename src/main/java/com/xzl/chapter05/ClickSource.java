package com.xzl.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author xzl
 * @create 2022-03-22 22:57
 */
public class ClickSource implements SourceFunction<Event> {

    //声明一个布尔变量 ，作为数据生成的标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            ctx.collect(new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis())
            );
            //隔一秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        running = false;

    }
}
