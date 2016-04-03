package cn.edu.ruc.cloudcomputing.book.chapter15;

import java.io.IOException;
import java.util.List;
 
import org.apache.zookeeper.KeeperException;
 
 public class DelGroup extends ZooKeeperInstance {
 	public void delete(String groupPath) throws KeeperException, InterruptedException{
		List<String> children=zk.getChildren(groupPath, false);
		//������գ������ɾ������
		if(!children.isEmpty()){
			//ɾ�����к��ӽڵ�
			for(String child:children)
				zk.delete(groupPath+"/"+child, -1);
		}
		//ɾ����Ŀ¼�ڵ�
		zk.delete(groupPath, -1);
	}
	
	public static void main(String args[]) throws IOException, KeeperException, InterruptedException{
		DelGroup dg=new DelGroup();
		dg.createZKInstance();
		dg.delete("/ZKGroup");
		dg.ZKclose();	
	}
}
