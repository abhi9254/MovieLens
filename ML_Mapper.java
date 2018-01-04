package movielens;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;

public class ML_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	static enum Stats {
		TOTAL, BAD
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		// URI[] files = context.getCacheFiles();

		CSVParser parser = new CSVParser();
		String[] record = parser.parseLine(line);
		if (record.length == 3) {
			context.getCounter(Stats.TOTAL).increment(1);
			String title = record[1];
			String genres = record[2];

			String[] list_genre = genres.split("\\|");

			for (String genre : list_genre) {
				context.write(new Text(genre.trim()), new Text(title.trim()));
			}

		} else {
			context.getCounter(Stats.BAD).increment(1);

		}
	}
}
