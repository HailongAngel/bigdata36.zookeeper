package cn.curd;

import java.util.List;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZKCURD {
	//zookeeper��ַ
	private static final String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	
	private int sessionTimeout = 2000;
	
	ZooKeeper zkClient = null;
	/**
	 * ��ʼ����Դ
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{
		zkClient= new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			public void process(WatchedEvent event) {
				System.out.println(event.getType() +"   "+  event.getPath());
				try {
					zkClient.getChildren("/", true);
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}
	
	/**
	 * ���ݵ�����
	 * @throws Exception 
	 */
	@Test
	public void testCreate() throws Exception{
		
		// 1:·��    2������    3��Ȩ��   4���ڵ������   ��ʱ�ĺͳ־õ�  ��EPHEMERAL PERSISTENT PERSISTENT_SEQUENTIAL EPHEMERAL_SEQUENTIAL��
		zkClient.create("/eclipse/e", "eclipse".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		//Thread.sleep(15000);
		
	}
	/**
	 * ��ȡ�ӽڵ�
	 * @throws Exception
	 */
	@Test
	public void getChildren() throws Exception{
		List<String> children = zkClient.getChildren("/", false);
		for (String string : children) {
			System.out.println(string);
		}
	}
	/**
	 * �жϽڵ��Ƿ����
	 * @throws Exception
	 */
	@Test
	public void testExist() throws Exception{
		Stat exists = zkClient.exists("/eclipse", false);
		System.out.println(exists);
	}
	/**
	 * ��ȡ�ڵ�����
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void testGetData() throws KeeperException, InterruptedException{
		byte[] data = zkClient.getData("/eclipse", false, null);
		System.out.println(new String(data));
	}
	/**
	 * ɾ���ڵ�
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	@Test
	public void  deleteData() throws InterruptedException, KeeperException{
		zkClient.delete("/w", -1);
		
	}
	/**
	 * �޸�����
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	@Test
	public void setData() throws KeeperException, InterruptedException{
		zkClient.setData("/w", "hello".getBytes(), -1);
	}
	/**
	 * ����
	 * @throws Exception
	 */
	@Test
	public void watch() throws Exception{
		List<String> children = zkClient.getChildren("/", true);
		for (String string : children) {
			System.out.println(string);
		}
		
		Thread.sleep(2*60*1000);
	}
	
	/**
	 * �ͷ���Դ
	 * @throws Exception
	 */
	@After
	public void close() throws Exception{
		zkClient.close();
	}

}
