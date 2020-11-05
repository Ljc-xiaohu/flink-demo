package cn.itcast.cron;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Date 2020/11/4 16:39
 * Desc 扩展: 演示JavaSE的定时任务API
 */
public class TestTimedTasks {
    public static void main(String[] args) {
        Timer timer = new Timer();
        //schedule(TimerTask task, long delay, long period)
        //schedule(要执行的任务, 延迟时间, 每隔多久执行一次)
        timer.schedule(new TimerTask(){
            @Override
            public void run() {
                System.out.println("5s后 每隔1s 执行一次");
            }
        }, 5000,1000 );

        ScheduledExecutorService service = Executors.newScheduledThreadPool(3);
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("5s后 每隔1s 执行一次");
            }
        }, 5,1, TimeUnit.SECONDS);

    }
}
