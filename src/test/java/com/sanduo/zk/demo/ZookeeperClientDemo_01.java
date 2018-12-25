package com.sanduo.zk.demo;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * zookeeper 存储功能演示
 * 
 * @author sanduo
 * @date 2018/09/29
 */
public class ZookeeperClientDemo_01 {

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

        // 创建客户端
        /**
         * connectString：e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"（2181是客户端通信端口）
         * <p/>
         * sessionTimeout：session timeout in milliseconds
         * <p/>
         * watcher a watcher： 监听通知，处理改变
         */
        zkClient = new ZooKeeper("hadoop01:2181,hadoop02:2181,hadoop03:2181", 2000, null);
        set(zkClient);
    }

    // 增
    @Test
    public void testAdd() throws KeeperException, InterruptedException {
        /**
         * String path : 创建的节点路径
         * <p/>
         * byte[] data ：数据-可以使用任何文件（数据太小，最大允许1M）-----先发到leader，再由leader 发给follower
         * <p/>
         * List<ACL> acl:权限，访问权限列表 是一个常量类 常量
         * <p/>
         * CreateMode createMode :节点类型 -创建模式
         */
        String create_path =
            zkClient.create("/eclipse", "hello eclipse".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(create_path);

    }

    /**
     * 改 if the given version is -1, it matches any node's versions
     * <p/>
     * 参数1： 节点路径
     * <p>
     * 参数2：传递的数据
     * <p>
     * 参数3：版本 -1 是所有版本
     * 
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void testUpdate() throws KeeperException, InterruptedException {
        zkClient.setData("/eclipse", "你好，Zookeeper".getBytes(Charset.defaultCharset()), -1);
    }

    // 查
    // 参数1：节点路径 参数2：要监听吗 参数3：获取所需的数据版本，null表示最新版本
    @Test
    public void testGet() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/eclipse", false, null);
        // 反编译数据
        System.out.println(new String(data, Charset.defaultCharset()));
    }

    // 查询所有子节点
    @Test
    public void testListChildren() throws KeeperException, InterruptedException {
        // 参数1：节点路径 参数2：要监听吗 参数3：获取所需的数据版本
        // 注意，返回的结果是子节点的名字，不带全路径
        List<String> children = zkClient.getChildren("/aa", false, null);
        for (String child : children) {
            System.out.println(child);
        }
    }

    // 删
    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        // 参数1：删除的路径 参数2：删除的版本 -1 代表所有版本
        zkClient.delete("/eclipse", -1);
    }
    // 监听端口

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
