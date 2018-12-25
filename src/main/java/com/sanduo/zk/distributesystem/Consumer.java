package com.sanduo.zk.distributesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * 消费者
 * 
 * @author sanduo
 * @date 2018/09/29
 */
public class Consumer {
    // 定义一个list存放最新的服务列表volatile使其在各个线程之间同步
    private volatile List<String> onlineServerList = new ArrayList<String>();

    ZooKeeper zk = null;

    // 构造zk客户端连接zk
    public void connectZk() throws IOException {
        zk = new ZooKeeper("hadoop01:2181,hadoop02:2181,hadoop03:2181", 2000, new Watcher() {
            // 监听处理逻辑
            public void process(WatchedEvent event) {
                // 再查一次注册监听
                if (event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeChildrenChanged) {

                    try {
                        // 事件回调中，在此查询zk上的在线服务器节点即可，查询逻辑中再次注册了子节点变化事件监听
                        getOnlineServers();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    // 查询服务器列表
    public void getOnlineServers() throws Exception {
        List<String> children = zk.getChildren("/servers", true);
        // 保证每次查询都是最新的服务器信息
        List<String> servers = new ArrayList<String>();

        for (String child : children) {
            byte[] data = zk.getData("/servers/" + child, false, null);

            servers.add(new String(data));
        }
        onlineServerList = servers;
        System.out.println("查询了一次zk，当前在线的服务器有：" + servers);
    }

    // 处理业务（向服务端请求时间查询）
    public void sendRequest() {
        Random random = new Random();
        System.out.println("消费者开始处理业务逻辑............");
        while (true) {
            try {
                // 选一台在线的服务器
                int index = random.nextInt(onlineServerList.size());
                String server = onlineServerList.get(index);
                String hostname = server.split(":")[0];
                int port = Integer.parseInt(server.split(":")[1]);

                System.out.println("本次请求的服务器是： " + server);

                // 接受信息和发送信息

                Socket socket = new Socket(hostname, port);

                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                out.write("haha".getBytes());
                out.flush();

                byte[] buf = new byte[256];

                int read = in.read(buf);
                System.out.println("服务器响应时间为：" + new String(buf, 0, read));

                out.close();
                in.close();
                socket.close();

                Thread.sleep(2000);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Consumer consumer = new Consumer();
        // 构造zk连接对象
        consumer.connectZk();
        // 查询服务器列表
        consumer.getOnlineServers();
        // 处理业务（向服务端请求时间查询）
        consumer.sendRequest();
    }

}
