package cn.edu.ruc.cloudcomputing.book.chapter15;

import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperInstance {
 	//�Ự��ʱʱ�䣬����Ϊ��ϵͳĬ��ʱ��һ��
 	public static final int SESSION_TIMEOUT=30000;
 
	//����ZooKeeperʵ��
	ZooKeeper zk;
	
	//����Watcherʵ��
	Watcher wh=new Watcher(){
		public void process(WatchedEvent event){
			System.out.println(event.toString());
		}
	};
	
 //��ʼ��Zookeeperʵ��
	public void createZKInstance() throws IOException{	
		zk=new ZooKeeper("localhost:2181",ZooKeeperInstance.SESSION_TIMEOUT,this.wh);
	}
	
	//�ر�ZKʵ��
	public void ZKclose() throws InterruptedException{
		zk.close();
	}
}
