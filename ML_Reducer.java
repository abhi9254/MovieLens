package movielens;

import java.io.IOException;

import movielens.ML_Mapper.Stats;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class ML_Reducer extends Reducer<Text, Text, Text, FloatWritable> {

	// Reducer will give genre and %age of movies in that
	public void reduce(Text genre, Iterable<Text> movies, Context context)
			throws IOException, InterruptedException {

		Cluster cluster = new Cluster(context.getConfiguration());
		Job currentJob = cluster.getJob(context.getJobID());
		int total_movies = (int) currentJob.getCounters()
				.findCounter(Stats.TOTAL).getValue();

		int count = 0;
		for (@SuppressWarnings("unused") Text movie : movies) {
			count++;
		}

		float percentage_dist = (count * 100) / total_movies;
		context.write(genre, new FloatWritable(percentage_dist));
	}
}
