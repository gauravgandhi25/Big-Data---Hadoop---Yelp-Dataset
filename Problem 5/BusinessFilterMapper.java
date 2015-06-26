import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessFilterMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	static String total_record = "";

	@Override
	protected void map(LongWritable baseAddress, Text line, Context context)
			throws IOException, InterruptedException {

		Text business_id = new Text();
		Text city = new Text();
		total_record = total_record.concat(line.toString());
		String[] fields = total_record.split("::");
		if (fields.length == 24) {
			if ((fields[22].equalsIgnoreCase("business"))) {
				business_id.set(fields[2].trim());
				city.set(fields[12].trim());
				context.write(business_id, city);
			}
			total_record = "";
		}
	}
}
