import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EntityCount {

	public static class EntityCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		static String total_record = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {

			Text entity = new Text();
			IntWritable one = new IntWritable(1);

			total_record = total_record.concat(line.toString());
			String[] fields = total_record.split("::");
			if (fields.length == 24) {
				entity.set(fields[22].trim());
				context.write(entity, one);
				total_record = "";
			}
		}
	}

	public static class EntityCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text entity, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if(entity.toString().equalsIgnoreCase("type"))
				return;
			IntWritable count = new IntWritable(0);
			int total = 0;
			for (IntWritable value : values) {
				total += value.get();
			}
			count.set(total);
			context.write(entity, count);
		}
	}

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
		Job job = new Job(conf, "Yelp Entity Count");

		job.setJarByClass(EntityCount.class);

		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);

		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(EntityCountMapper.class);
		job.setReducerClass(EntityCountReducer.class);
		job.setCombinerClass(EntityCountReducer.class);

		FileInputFormat.setMinInputSplitSize(job, 500000000);
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}
}