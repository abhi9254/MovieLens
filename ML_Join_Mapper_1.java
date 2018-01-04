package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;

public class ML_Join_Mapper_1 extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	static enum Stats {
		TOTAL, BAD
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		// URI[] files = context.getCacheFiles();

		CSVParser parser = new CSVParser();
		String[] record = parser.parseLine(line);
		if (record.length == 3 && !"movieId".equals(record[0])) {
			context.getCounter(Stats.TOTAL).increment(1);
			Integer movie_id = Integer.parseInt(record[0].trim());
			String genres = record[2];

			context.write(new IntWritable(movie_id), new Text("movies\t"
					+ genres.trim()));

		} else {
			context.getCounter(Stats.BAD).increment(1);

		}
	}

}
