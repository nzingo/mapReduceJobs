import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NormalizeMapperOne
	extends Mapper<Object, Text, CompositeKey, Text>{

	public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
		

		String[] tokens = value.toString().split(",");

		int import_value = Integer.parseInt(tokens[1]);

		// prepare reducer key
		CompositeKey reducerKey = new CompositeKey();
		reducerKey.setNaturalKey("same");
		reducerKey.setSecondaryKey(import_value); // inject value into key

		// send it to reducer      
		context.write(reducerKey, value);
		//emit(reducerKey, new IntWritable(temperature));

	}
}
	