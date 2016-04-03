package mr.topK;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopK {

	public static final int K = 2;
	
	public static class KMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		TreeMap<Integer, String> map = new TreeMap<Integer, String>(); 
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			if(line.trim().length() > 0 && line.indexOf("\t") != -1) {
				
				String[] arr = line.split("\t", 2);
				String name = arr[0];
				Integer num = Integer.parseInt(arr[1]);
				map.put(num, name);
				
				if(map.size() > K) {
					map.remove(map.firstKey());
				}
			}
		}

		//由mapper源码可知，在任务的最后阶段被调用执行，
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			for(Integer num : map.keySet()) {
				context.write(new IntWritable(num), new Text(map.get(num)));
			}
			
		}
		
	}
	
	
	public static class KReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		
		TreeMap<Integer, String> map = new TreeMap<Integer, String>();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				
			map.put(key.get(), values.iterator().next().toString());
			if(map.size() > K) {
				map.remove(map.firstKey());
			}
		}

		@Override
		protected void cleanup(
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for(Integer num : map.keySet()) {
				context.write(new IntWritable(num), new Text(map.get(num)));
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf, "my own word count");
			job.setJarByClass(TopK.class);
			job.setMapperClass(KMap.class);
			job.setCombinerClass(KReduce.class);
			job.setReducerClass(KReduce.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, new Path("./in-files/topK.txt"));
			FileOutputFormat.setOutputPath(job, new Path("./out-topk"));
			System.out.println(job.waitForCompletion(true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
