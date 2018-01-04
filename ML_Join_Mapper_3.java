package movielens;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ML_Join_Mapper_3 extends
		Mapper<LongWritable, Text, Text, FloatWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] record = value.toString().split("\t");
		String genre = record[0];
		Float rating = Float.parseFloat(record[1]);
		context.write(new Text(genre), new FloatWritable(rating));

	}

}
