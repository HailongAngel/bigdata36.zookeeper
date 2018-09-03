package cn.sense;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Consume {
	List<String> onlineServers;
	private static final String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	 ZooKeeper zk = null;
  public static void main(String[] args) throws Exception {
	  Consume consume = new Consume();
	//1.连接zookeeper
	  consume.connectZK();
	 //2.查询在线服务器，并注册监听
	  consume.getOnlineServers();
	  //3.挑选服务器请求业务
	  consume.requestServers();
}
  
  public void connectZK() throws Exception {
	  zk = new ZooKeeper(connectString, 2000, new Watcher() {
		
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			try {
				getOnlineServers();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	});
 }
  
  public void getOnlineServers() throws Exception, InterruptedException {
	  List<String> serverList = new ArrayList<String>();//这里面放一个List防止用户挑选服务器的时候这边还一直在运行
	  List<String> children = zk.getChildren("/servers", true);
	  for (String child : children) {
		  byte[] data = zk.getData("/servers/"+child, true, null);
		  String server = new String(data);
		  serverList.add(server);
	}
	  onlineServers = serverList;//将这一阶段的值放入总的里面
  }
  
  public void requestServers() throws Exception {
	  Random random = new Random();
	  while(true) {
		  Thread.sleep(2000);
		  if(onlineServers.size()==0) {
			  System.out.println("还没有在线的服务器");
			  continue;
		  }
		  String server = onlineServers.get(random.nextInt(onlineServers.size()));
		  System.out.println("本次挑选了服务器"+server);
		  
	  }
  }
}
