import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessFilterReducer extends Reducer<Text, Text, Text, Text> {

	HashMap<String, Float> map = new HashMap<String, Float>();

	@Override
	protected void reduce(Text business_id, Iterable<Text> address,
			Context context) throws IOException, InterruptedException {

		for (Text add : address) {
			context.write(business_id, add);
		}

	}
}