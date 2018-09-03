package cn.sense;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Provider {
	private static final String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	 ZooKeeper zk = null;
	public static void main(String[] args) throws Exception {
		Provider provider = new Provider();
		
		
		//1.��ȡzk����
		provider.connectZK();
		
		
		//2.ע���������Ϣ
		
		provider.registServerInfo();
		
		//3.�ȴ����󣬴���ҵ��
		provider.handleService();
	}
     public void connectZK() throws Exception {
    	  zk = new ZooKeeper(connectString, 2000, new Watcher() {
			
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				
			}
		});
     }
     
     public void registServerInfo() throws Exception {
    	 String hostName = InetAddress.getLocalHost().getHostName();
    	 
    	 if(zk.exists("/servers", false)==null) {
    		 zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    	 }
    	 String path = zk.create("/servers/server", (hostName+":"+8080).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    	 System.out.println(hostName+"�ɹ�ע����һ���ڵ�"+path);
    	 
     }
     
     public void handleService() throws Exception {
    	 ServerSocket ss = new ServerSocket(8080);
    	 while(true) {
    		 Socket sc = ss.accept();
    	 }
     }
     
}
