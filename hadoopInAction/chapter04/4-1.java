package cn.edu.ruc.cloudcomputing.book.chapter04;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); //������Ĵ��ı��ļ�������ת����String
			System.out.println(line);//Ϊ�˱��ڳ���ĵ��ԣ�������������   
                //������������Ȱ��н��зָ�
			StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");		 
	         //�ֱ��ÿһ�н��д���
			while(tokenizerArticle.hasMoreTokens()){
                   //ÿ�а��ո񻮷�
				StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken()); 
				String strName = tokenizerLine.nextToken(); //ѧ����������  
				String strScore = tokenizerLine.nextToken();//�ɼ�����
				Text name = new Text(strName);//ѧ������  
				int scoreInt = Integer.parseInt(strScore);//ѧ���ɼ�score of student
				context.write(name, new IntWritable(scoreInt));//��������ͳɼ�
			}
	    }
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			int count=0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();	//�����ܷ�
				count++;//ͳ���ܵĿ�Ŀ��
			} 
			int average = (int) sum/count;//����ƽ���ɼ�
			context.write(key, new IntWritable(average));
		}
	} 
	public int run(String [] args) throws Exception {
	     Job job = new Job(getConf());
	     job.setJarByClass(Score_Process.class);
	     job.setJobName("Score_Process");
	     job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(IntWritable.class);
	     job.setMapperClass(Map.class);
	     job.setCombinerClass(Reduce.class);
	     job.setReducerClass(Reduce.class);
	     job.setInputFormatClass(TextInputFormat.class);
	     job.setOutputFormatClass(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(job, new Path(args[0]));
	     FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     boolean success = job.waitForCompletion(true);
	     return success ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
	     int ret = ToolRunner.run(new Score_Process(), args);
	     System.exit(ret);
	}
}


