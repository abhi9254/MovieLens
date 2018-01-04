package movielens;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class ML_Join_Reducer_2 extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {

	// Reducer will give genre and its overall average rating
	public void reduce(Text genre, Iterable<FloatWritable> ratings,
			Context context) throws IOException, InterruptedException {

		Integer count = 0;
		Float rating = (float) 0;

		for (FloatWritable rated : ratings) {

			rating += rated.get();
			count++;
		}

		if (count != 0) {
			Float avg_rating = (float) rating / count;

			context.write(new Text(genre), new FloatWritable(avg_rating));

		}
	}

}
