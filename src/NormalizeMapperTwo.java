import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NormalizeMapperTwo
	extends Mapper<Object, Text, CompositeKeyDescend, Text>{
	
	public void map(Object key, Text value, Context context
	         ) throws IOException, InterruptedException {
		
	
		String[] tokens = value.toString().split(",");
	
		int balance = Integer.parseInt(tokens[2]);
	
		// prepare reducer key
		CompositeKeyDescend reducerKey = new CompositeKeyDescend();
		reducerKey.setNaturalKey("same");
		reducerKey.setSecondaryKey(balance); // inject value into key
	
		// send it to reducer      
		context.write(reducerKey, value);
		//emit(reducerKey, new IntWritable(temperature));
	
	}
}