package movielens;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ML_Join_Driver {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: ML_Join_Driver <input path movies> <input path ratings> <output path>");
			System.exit(-1);
		}

		JobConf jobc = new JobConf();
		FileOutputFormat.setOutputPath(jobc, new Path(args[2] + "_tmp"));

		JobConf jobc2 = new JobConf();
		FileInputFormat.addInputPath(jobc2, new Path(args[2] + "_tmp"));
		FileOutputFormat.setOutputPath(jobc2, new Path(args[2]));

		Job job = Job.getInstance(jobc);
		Job job2 = Job.getInstance(jobc2);

		job.addCacheFile(new URI("/user/cloudera/lib/opencsv-4.0.jar"));
		job.addCacheFile(new URI("/user/cloudera/lib/commons-lang3-3.6.jar"));
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, ML_Join_Mapper_1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, ML_Join_Mapper_2.class);
		job.setNumReduceTasks(3); // -D mapred.reduce.tasks=10
		job.setJarByClass(ML_Join_Driver.class);
		job.setJobName("ML_Join_Driver Job");
		job.setReducerClass(ML_Join_Reducer_1.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job2.setNumReduceTasks(1); // -D mapred.reduce.tasks=10
		job2.setJarByClass(ML_Join_Driver.class);
		job2.setJobName("ML_Join_Driver_stage_2 Job");
		job2.setMapperClass(ML_Join_Mapper_3.class);
		job2.setReducerClass(ML_Join_Reducer_2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(FloatWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(FloatWritable.class);

		int job1 = (job.waitForCompletion(true) ? 0 : 1);
		if (job1 == 0) {
			System.out.println("Job 1 success");
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		} else {
			System.out.println("Job 1 failed");
			System.exit(1);
		}
	}
}
