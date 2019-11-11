import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FinalIndexMapper 
	extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context context )
			throws IOException, InterruptedException{
		
		String[] tokens = value.toString().split(",");
		
		Text textKey = new Text();
		Text textValue = new Text();
		
		String Reporter = tokens[0];
		
		
		float idx1 = Float.parseFloat(tokens[4]);
		float idx2 = Float.parseFloat(tokens[5]);
		float idx3 = Float.parseFloat(tokens[6]);
		
		float idx = (float) (0.8 * idx1 + 0.05 * idx2 + 0.15 * idx3);
		String index = String.format ("%.2f", idx);
					
		String list = tokens[1]+","+tokens[2]+","+tokens[3]+","+tokens[4]+","+tokens[5]+","+tokens[6]+","+index;
		
		textKey.set(Reporter);
		textValue.set(list);
		
		context.write(textKey,textValue);
					
	}		
}