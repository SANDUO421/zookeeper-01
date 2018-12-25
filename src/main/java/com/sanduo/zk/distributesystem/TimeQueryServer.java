package com.sanduo.zk.distributesystem;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 分布式系统-服务端
 * 
 * @author sanduo
 * @date 2018/09/29
 */
public class TimeQueryServer {
    ZooKeeper zk = null;
    // 构造zk客户端连接zk

    public void connectZk() throws IOException {
        zk = new ZooKeeper("hadoop01:2181,hadoop02:2181,hadoop03:2181", 2000, null);
    }

    // 注册服务器信息
    public void registerServerInfo(String hostname, String port) throws KeeperException, InterruptedException {
        /**
         * 先判断注册节点的父节点是否存在，不存在则创建
         * 
         */
        if (zk.exists("/servers", false) == null) {
            // 不存在
            zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 存在，创建子节点,短暂带序号
        String node = zk.create("/servers/server", (hostname + ":" + port).getBytes(), Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + " 服务器向zk注册信息成功，注册的节点为：" + node);

    }

    public static void main(String[] args) throws Exception {
        TimeQueryServer timeQueryServer = new TimeQueryServer();
        // 构造zk客户端连接zk
        timeQueryServer.connectZk();
        // 注册服务器信息
        timeQueryServer.registerServerInfo(args[0], args[1]);
        // 启动业务线程开始处理业务
        new TimeQueryService(Integer.parseInt(args[1])).start();
    }
}
