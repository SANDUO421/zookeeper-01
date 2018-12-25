package com.sanduo.zk.distributesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

/**
 * 业务类
 * 
 * @author sanduo
 * @date 2018/09/29
 */
public class TimeQueryService extends Thread {

    int port = 0;

    public TimeQueryService(int port) {
        this.port = port;
    }

    /* 业务
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
        ServerSocket serverSocket = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("业务线程已经绑定端口" + port + "准备接受消费端请求了.......");
            while (true) {
                Socket accept = serverSocket.accept();
                in = accept.getInputStream();
                out = accept.getOutputStream();
                out.write(new Date().toString().getBytes());
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != out) {
                    out.close();
                }
                if (null != in) {
                    in.close();
                }
                if (null != serverSocket) {
                    if (!serverSocket.isClosed()) {
                        serverSocket.close();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
