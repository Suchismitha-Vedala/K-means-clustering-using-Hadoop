package homework1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DataProcessing {

	public static class DataProcessingMapper extends Mapper<LongWritable, Text, Text, Text> {

		private int count;
		private String pID;
		private String uID;
		private String rating;

		public DataProcessingMapper() {
			count = 0;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			count++;
			String v = value.toString();

			if (value.find("product/productId:") >= 0)
				pID = v.split(" ")[1];
			else if (value.find("review/userId:") >= 0)
				uID = v.split(" ")[1];
			else if (value.find("review/score:") >= 0)
				rating = v.split(" ")[1];

			if (count == 11) {
				context.write(new Text(pID), new Text(String.format("(%s,%s)", uID, rating)));
			}
		}

	}

	public static class DataProcessingReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String sum = new String();

			for (Text text : values) {
				sum = sum + text.toString() + ";";
			}

			context.write(key, new Text(sum));
		}
	}

}
