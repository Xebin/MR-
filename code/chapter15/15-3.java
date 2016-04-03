package cn.edu.ruc.cloudcomputing.book.chapter15;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
 
public class CreateGroup extends ZooKeeperInstance {
	
	//������
	//������groupPath
	public void createPNode(String groupPath) throws KeeperException, InterruptedException{		
		//������
		String cGroupPath=zk.create(groupPath, "group".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		//�����·��
		System.out.println("��������·��Ϊ��"+cGroupPath);
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException{
		CreateGroup cg=new CreateGroup();
		cg.createZKInstance();
		cg.createPNode("/ZKGroup");
		cg.ZKclose();
	}
}
