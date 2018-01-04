package movielens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ML_Join_Mapper_2 extends
		Mapper<LongWritable, Text, IntWritable, Text> {

	static enum Stats {
		TOTAL, BAD
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		String[] record = line.split(",");
		if (record.length == 4 && !"userId".equals(record[0])) {
			context.getCounter(Stats.TOTAL).increment(1);
			Integer movie_id = Integer.parseInt(record[1].trim());
			String rating = record[2];

			context.write(new IntWritable(movie_id), new Text("ratings\t"
					+ rating.trim()));

		} else {
			context.getCounter(Stats.BAD).increment(1);

		}
	}

}
