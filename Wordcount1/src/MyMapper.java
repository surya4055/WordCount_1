import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper  extends Mapper <LongWritable ,Text,Text,IntWritable >{

	//private final IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		System.out.println("Key :" + key.get());
			String words[] = value.toString().split(" ");
			System.out.println("Value " +value.toString());
			
			for(String Words:words) {
			context.write(new Text(Words),new IntWritable(1));
			}}
	
	public static void main(String[] args) {
		
	}}
