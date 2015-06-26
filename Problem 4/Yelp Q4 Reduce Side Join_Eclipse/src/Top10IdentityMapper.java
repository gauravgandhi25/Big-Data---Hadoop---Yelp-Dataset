import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Top10IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text line,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		if(line.getLength()>0)
		{
			String[] fields=line.toString().split("\t");	
			String value= "a:\t"+fields[1];
			context.write(new Text(fields[0].trim()), new Text(value));
		}
	}
	

}
