import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PaloAltoFilter {
	public static class PaloAltoFilterMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		static String total_record = "";

		@Override
		protected void map(LongWritable baseAddress, Text line, Context context)
				throws IOException, InterruptedException {

			Text business_id = new Text();
			total_record = total_record.concat(line.toString());
			String[] fields = total_record.split("::");
			if (fields.length == 24) {
				if ((fields[22].equalsIgnoreCase("business"))
						&& fields[3].contains("Palo Alto")) {
					business_id.set(fields[2].trim());
					NullWritable nullOb = NullWritable.get();
					context.write(business_id, nullOb);
				}
				total_record = "";
			}
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
		Job job = new Job(conf, "Palo Alto Count");

		job.setJarByClass(PaloAltoFilter.class);

		Path inputFile = new Path(otherArgs[0]);
		Path outputFile = new Path(otherArgs[1]);

		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(PaloAltoFilterMapper.class);
		job.setNumReduceTasks(0);
		FileInputFormat.setMinInputSplitSize(job, 500000000);

		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
