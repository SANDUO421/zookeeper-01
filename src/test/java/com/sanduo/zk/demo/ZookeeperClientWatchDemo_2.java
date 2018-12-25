package com.sanduo.zk.demo;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * zookeeper 监视功能
 * 
 * @author sanduo
 * @date 2018/09/29
 */
public class ZookeeperClientWatchDemo_2 {

    private ZooKeeper zkClient = null;

    /**
     * @param zkClient
     */
    public void set(ZooKeeper zkClient) {
        this.zkClient = zkClient;
    }

    /**
     * 初始化
     * 
     * @throws IOException
     */
    @Before
    public void setup() throws IOException {

        // 创建客户端：创建了两个线程，一个用户收发数据，一个用户监听数据变化
        /**
         * connectString：e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"（2181是客户端通信端口）
         * <p/>
         * sessionTimeout：session timeout in milliseconds
         * <p/>
         * watcher a watcher： 监听通知，处理改变
         */
        // zkClient = new ZooKeeper("hadoop01:2181,hadoop02:2181,hadoop03:2181", 2000, null); //一次监听
        zkClient = new ZooKeeper("hadoop01:2181,hadoop02:2181,hadoop03:2181", 2000, new Watcher() {
            // 监听
            public void process(WatchedEvent event) {
                // 连接状态，数据改变
                if (event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeDataChanged) {

                    System.out.println(event.getPath());// 事件发生的节点路径
                    System.out.println(event.getType());// 发生的事件类型
                    System.out.println("赶快刹车......前面有人！");// 事假发生后的操作

                    // 继续监听
                    try {
                        zkClient.getData("/watch", true, null);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (event.getState() == KeeperState.SyncConnected
                    && event.getType() == EventType.NodeChildrenChanged) {
                    // 监听子节点变化
                    System.out.println("子节点变化了！");
                    try {
                        zkClient.getChildren("/watch", true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }); // 反复监听
        set(zkClient);
    }

    /**
     * 监听端口 只是监听一次
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testGetWatch() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/watch", new Watcher() {
            // 监听
            public void process(WatchedEvent event) {
                System.out.println(event.getPath());// 事件发生的节点路径
                System.out.println(event.getType());// 发生的事件类型
                System.out.println("赶快刹车......前面有人！");// 事假发生后的操作
            }
        }, null);

        System.out.println(new String(data, Charset.defaultCharset()));

        Thread.sleep(Long.MAX_VALUE);// 这个线程睡眠
    }

    /**
     * 反复监听： 此时需要将监听器注册初始化
     */
    @Test
    public void testGetAlwaysWatch() throws KeeperException, InterruptedException {
        // 调的就是构造的时候的回调逻辑
        byte[] data = zkClient.getData("/watch", true, null);// 监听节点数据变化
        List<String> children = zkClient.getChildren("/watch", true);
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println(new String(data, Charset.defaultCharset()));
        // 因为zookeeper 中中启动的监听线程就是守护线程，所以保证主线程一定要启动
        Thread.sleep(Long.MAX_VALUE);// 这个线程睡眠
    }

    /**
     * 资源释放
     * 
     * @throws InterruptedException
     * 
     */
    @After
    public void close() throws InterruptedException {
        if (zkClient != null) {
            zkClient.close();
        }
    }

}
