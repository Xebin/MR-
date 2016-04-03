package cn.edu.ruc.cloudcomputing.book.chapter07;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadFile {
public static void main(String[] args) throws IOException {
String uri = "����Ҫ��ȡ��SequenceFile����λ��";
	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(URI.create(uri), conf);
	Path path = new Path(uri);
	SequenceFile.Reader reader = null;
	try {
	reader = new SequenceFile.Reader(fs, path, conf);
Writable key =(Writable)ReflectionUtils.newInsta
nce(reader.getKeyClass(), conf);
		Writable value =(WritableReflectionUtils.newInsta
nce(reader.getValueClass(), conf);
		long position = reader.getPosition();
		while (reader.next(key, value)) {
		String syncSeen = reader.syncSeen() ? "*" : "";
		System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
		position = reader.getPosition(); // beginning of next record
		}
	} finally {
	IOUtils.closeStream(reader);
	}
}
}
