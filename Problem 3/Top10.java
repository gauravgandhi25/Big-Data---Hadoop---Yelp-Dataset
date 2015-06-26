import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10 {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Incompatible Number Of Arguments");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Yelp Top 10 Rated Business");

		job.setJarByClass(Top10.class);

		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);

		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(Top10_Mapper.class);
		job.setReducerClass(Top10_Reducer.class);

		FileInputFormat.setMinInputSplitSize(job, 500000000);

		System.exit(job.waitForCompletion(true) ? 1 : 0);

	}

	public static class Top10_Reducer extends
			Reducer<Text, IntWritable, Text, FloatWritable> {

		HashMap<String, Float> map = new HashMap<String, Float>();

		@Override
		protected void reduce(Text business_id, Iterable<IntWritable> stars,
				Context context) throws IOException, InterruptedException {

			FloatWritable average = new FloatWritable(0);
			int total = 0;
			int count = 0;
			for (IntWritable star : stars) {
				total += star.get();
				count++;
			}

			float avg = total / count;
			average.set(avg);
			map.put(business_id.toString(), avg);
		}

		@Override
		protected void cleanup(
				Reducer<Text, IntWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {

			Map<String, Float> sortedMap = new TreeMap<String, Float>(
					new ValueComparator(map));
			sortedMap.putAll(map);
			int i = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()),
						new FloatWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
		}
	}

	public static class Top10_Mapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		static String total_record = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {

			Text business_id = new Text();
			IntWritable stars = new IntWritable(1);

			total_record = total_record.concat(line.toString());
			String[] fields = total_record.split("::");
			if (fields.length == 24) {
				if ((fields[22].equalsIgnoreCase("review"))) {
					business_id.set(fields[2].trim());
					stars.set(Integer.parseInt(fields[20].trim()));
					context.write(business_id, stars);
				}
				total_record = "";
			}
		}
	}

}
