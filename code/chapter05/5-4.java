package cn.edu.ruc.cloudcomputing.book.chapter05;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class STjoin {
  public static int time = 0;
//map������ָ��child��parent��Ȼ���������һ����Ϊ�ұ���//�����һ����Ϊ�����Ҫע������������value�б���������ұ�//�����־
  public static class Map extends Mapper<Object, Text, Text, Text>{
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       String childname = new String();
       String parentname = new String();
       String relationtype = new String();
       String line = value.toString();
       int i = 0;
       while(line.charAt(i)!=' '){
		i++;
	}
       String[] values = {line.substring(0,i),line.substring(i+1)};
       if(values[0].compareTo("child") != 0)
       {
    	   childname = values[0];
    	   parentname = values[1];
    	   relationtype = "1";  //���ұ����ֱ�־
    	   context.write(new Text(values[1]), new Text(relationtype + "+" + childname + "+" + parentname));
         //���
    	   relationtype = "2";  
    	   context.write(new Text(values[0]), new Text(relationtype + "+" + childname + "+" + parentname));
      //�ұ� 
       }
    }
  }
  
  public static class Reduce extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	
	 if(time == 0){   //�����ͷ
	 	context.write(new Text("grandchild"),new Text("grandparent"));
		time++;
	 }
         int grandchildnum = 0;
    	 String grandchild[] = new String[10];
    	 int grandparentnum = 0;
    	 String grandparent[] = new String[10];
	 Iterator ite = values.iterator();
    	 while(ite.hasNext())
    	 {
    		 String record = ite.next().toString();
    		 int len = record.length();
    		 int i = 2;
		 	if(len == 0) continue;
    		 char relationtype = record.charAt(0);
    		 String childname = new String();
    		 String parentname = new String();
			//��ȡvalue-list��value��child
    		 while(record.charAt(i) != '+')
    		 {
    			 childname = childname + record.charAt(i); 	
    			 i++;		 
    	         }
    		 i = i+1;
			//��ȡvalue-list��value��parent
    		 while(i < len)
    		 {
    			 parentname = parentname + record.charAt(i);	
    			 i++;	
    		 }
			//���ȡ��child����grandchild
    		 if(relationtype == '1'){
    			 grandchild[grandchildnum] = childname;
    			 grandchildnum++;
    		 }
    		 else{//�ұ�ȡ��parent����grandparent
    			 grandparent[grandparentnum] = parentname;
    			 grandparentnum++;
    		 }
         }
		//grandchild��grandparent������ѿ�����
    	 if(grandparentnum != 0 && grandchildnum != 0){
    		 for(int m = 0; m < grandchildnum; m++){
    			 for(int n = 0; n < grandparentnum; n++){
    	context.write(new Text(grandchild[m]),new Text(grandparent[n]));   //������
    			 }
    		 }
    	}
    	 
	}
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "single table join");
    job.setJarByClass(STjoin.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
