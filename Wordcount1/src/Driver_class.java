import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver_class {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
		String i_path="hdfs://localhost:9000/user/hduser/MapR1";
		String o_path="hdfs://localhost:9000/user/hduser/MapR1_op";
	
		Path inputpath=new Path(i_path);
		Path outputpath=new Path(o_path);
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
			
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJarByClass(Driver_class.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
			
		FileInputFormat.addInputPath(job, inputpath);
	FileOutputFormat.setOutputPath(job, outputpath);
		outputpath.getFileSystem(conf).delete(outputpath,true);
		System.out.println("beofore deleting the output directory");
		Thread.sleep(30000);
		System.exit(job.waitForCompletion(true)?0:1);
	}}
