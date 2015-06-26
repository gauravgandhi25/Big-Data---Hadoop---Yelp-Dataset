import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Mapjoin_reducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text business_id, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		boolean flag = false;

		String dataA_str = "";
		String dataB_str = "";
		for (Text value : values) {
			if (value.toString().contains("a:")) {
				dataA_str = value.toString();
				flag = true;
			} else
				dataB_str = value.toString();
		}
		if (!flag)
			return;

		String[] dataB = dataB_str.split("\t");
		String[] dataA = dataA_str.split("\t");
		String data = dataB[1] + "\t" + dataB[2] + "\t" + dataA[1];
		context.write(business_id, new Text(data));
	}
}
