package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));
        if (log.contains("start")){//启动日志
            if (LogUtils.validateStart(log)){
                return event;
            }
        }else {//事件日志
            if (LogUtils.validateEvent(log)){
                return event;
            }
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> events = new ArrayList<>();

        for (Event event : list) {
            Event interceptEvent = intercept(event);
            if (interceptEvent != null){
                events.add(interceptEvent);
            }
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
