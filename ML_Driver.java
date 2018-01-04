package movielens;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class ML_Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: ML_Driver <input path> <output path>");
			System.exit(-1);
		}

		JobConf jobc = new JobConf();

		FileInputFormat.addInputPath(jobc, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobc, new Path(args[1]));

		Job job = Job.getInstance(jobc);
		job.addCacheFile(new URI("/user/cloudera/lib/opencsv-4.0.jar"));
		job.addCacheFile(new URI("/user/cloudera/lib/commons-lang3-3.6.jar"));
		job.setNumReduceTasks(3); //-D mapred.reduce.tasks=10
		// job.addCacheArchive(new URI("/user/cloudera/lib/libs.zip"));
		job.setJarByClass(ML_Driver.class);
		job.setJobName("ML_Driver Job");
		job.setMapperClass(ML_Mapper.class);
		job.setReducerClass(ML_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
