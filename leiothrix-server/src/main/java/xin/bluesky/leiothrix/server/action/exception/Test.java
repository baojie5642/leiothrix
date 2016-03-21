package xin.bluesky.leiothrix.server.action.exception;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author 张轲
 */
public class Test {
    public static void main(String[] args) throws Exception{
        ReentrantLock lock=new ReentrantLock();
        Condition condition=lock.newCondition();
        Thread t=new Thread(){
            public void run(){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lock.lock();
                condition.signal();
                System.out.println("after signal");
                lock.unlock();
            }
        };
//        t.start();

        lock.lock();
        boolean r=condition.await(5, TimeUnit.SECONDS);
        System.out.println(r);

        lock.lock();
        System.out.println("还能lock");
    }
}