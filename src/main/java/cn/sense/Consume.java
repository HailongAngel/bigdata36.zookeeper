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
	//1.����zookeeper
	  consume.connectZK();
	 //2.��ѯ���߷���������ע�����
	  consume.getOnlineServers();
	  //3.��ѡ����������ҵ��
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
	  List<String> serverList = new ArrayList<String>();//�������һ��List��ֹ�û���ѡ��������ʱ����߻�һֱ������
	  List<String> children = zk.getChildren("/servers", true);
	  for (String child : children) {
		  byte[] data = zk.getData("/servers/"+child, true, null);
		  String server = new String(data);
		  serverList.add(server);
	}
	  onlineServers = serverList;//����һ�׶ε�ֵ�����ܵ�����
  }
  
  public void requestServers() throws Exception {
	  Random random = new Random();
	  while(true) {
		  Thread.sleep(2000);
		  if(onlineServers.size()==0) {
			  System.out.println("��û�����ߵķ�����");
			  continue;
		  }
		  String server = onlineServers.get(random.nextInt(onlineServers.size()));
		  System.out.println("������ѡ�˷�����"+server);
		  
	  }
  }
}
