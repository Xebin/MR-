package cn.edu.ruc.cloudcomputing.book.chapter05;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class WordCount {
//�̳�Mapper�ӿڣ�����map����������Ϊ<Object,Text>
//�������Ϊ<Text��IntWritable>
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    //one��ʾ���ʳ���һ��
private final static IntWritable one = new IntWritable(1);
//word���ڴ洢���µĵ���
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());   //����������д�
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());  //���µĵ��ʴ���word
        context.write(word, one);
      }
    }
  }
//�̳�Reducer�ӿڣ�����Reduce����������Ϊ<Text,IntWritable>
//�������Ϊ<Text��IntWritable>

  public static class IntSumReducer   extends Reducer<Text,IntWritable,Text,IntWritable> {
 //result��¼���ʵ�Ƶ��
  private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
      int sum = 0;
		//�Ի�ȡ��<key,value-list>����value�ĺ�
      for (IntWritable val : values) {
        sum += val.get();
      }
		//��Ƶ�����õ�result��
      result.set(sum);
     //�ռ����
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
//�����������
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
}
//������ҵ��
Job job = new Job(conf, "word count");
//������ҵ�ĸ�����
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
