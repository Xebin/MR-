package cn.edu.ruc.cloudcomputing.book.chapter15;
 
import java.io.IOException;
 
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class JoinGroup extends ZooKeeperInstance {	
	//���������
	public int Join(String groupPath,int k) throws KeeperException, InterruptedException{
		String child=k+"";
		child="child_"+child;
		
		//������·��
		String path=groupPath+"/"+child;
		//������Ƿ����
		if(zk.exists(groupPath,true) != null){
			//������ڣ�������
			zk.create(path,child.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return 1;
		}
		else{
			System.out.println("�鲻���ڣ�");
			return 0;
		}
	}
	
	//���������
	public void MultiJoin() throws KeeperException, InterruptedException{
		for(int i=0;i<10;i++){
			int k=Join("/ZKGroup",i);
			//����鲻�������˳�
			if(0==k)
				System.exit(1);
		}
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException{
		JoinGroup jg=new JoinGroup();
		jg.createZKInstance();
		jg.MultiJoin();
		jg.ZKclose();
	}	
}
