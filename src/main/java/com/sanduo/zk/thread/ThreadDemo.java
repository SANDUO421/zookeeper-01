package com.sanduo.zk.thread;

/**
 * 子线程和守护线程的问题
 * 
 * @author sanduo
 * @date 2018/09/29
 */
public class ThreadDemo {

    public static void main(String[] args) {
        System.out.println("主线程启东.....");
        System.out.println("主线程准备启动子线程...");

        Thread thread = new Thread(new Runnable() {

            public void run() {
                System.out.println("子线程开始执行.......");
                while (true) {
                    try {
                        Thread.sleep(2000);
                        System.out.println("子线程开始打印.......");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        // 守护线程，只有当其他的线程都启动时，才会执行
        thread.setDaemon(true);
        thread.start();

        System.out.println("主线程子线程执行后语句...............");

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
