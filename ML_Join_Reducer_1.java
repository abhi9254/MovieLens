package movielens;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ML_Join_Reducer_1 extends
		Reducer<IntWritable, Text, Text, FloatWritable> {

	// Reducer will give genres and rating for each movie
	public void reduce(IntWritable movieId, Iterable<Text> movie_info,
			Context context) throws IOException, InterruptedException {

		Integer count = 0;
		Float rating = (float) 0;
		String[] genres = null;

		for (Text info : movie_info) {
			String[] info_list = info.toString().split("\t");
			// check which dataset is the record from
			if ("movies".equals(info_list[0])) {
				genres = info_list[1].split("\\|");
			}

			else {
				rating += Float.parseFloat(info_list[1]);
				count++;
			}
		}

		if (count != 0) {
			Float avg_rating = (float) rating / count;

			for (String genre : genres) {
				context.write(new Text(genre.trim()), new FloatWritable(
						avg_rating));
			}
		}
	}

}
