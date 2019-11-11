import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NormalizeMapperThree
	extends Mapper<Object, Text, CompositeKey, Text>{
	
	public void map(Object key, Text value, Context context
	         ) throws IOException, InterruptedException {
		
	
		String[] tokens = value.toString().split(",");
	
		float unit_value = Float.parseFloat(tokens[3]);
	
		// prepare reducer key
		CompositeKey reducerKey = new CompositeKey();
		reducerKey.setNaturalKey("same");
		reducerKey.setSecondaryKey((int)unit_value); // inject value into key
	
		// send it to reducer      
		context.write(reducerKey, value);
		//emit(reducerKey, new IntWritable(temperature));
	
	}
}