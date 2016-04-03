package cn.edu.ruc.cloudcomputing.book.chapter15;
 
import java.io.IOException;
import java.util.List;
 
import org.apache.zookeeper.KeeperException;
 
public class ListMembers extends ZooKeeperInstance {
	public void list(String groupPath) throws KeeperException, InterruptedException{
		//��ȡ�����ӽڵ�
		List<String> children=zk.getChildren(groupPath, false);
		if(children.isEmpty()){
			System.out.println("��"+groupPath+"��û�����Ա���ڣ�");
			System.exit(1);
		}
		for(String child:children)
			System.out.println(child);		
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException{
		ListMembers lm=new ListMembers();
		lm.createZKInstance();
		lm.list("/ZKGroup");
	}
}
