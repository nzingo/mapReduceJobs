import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class cleanMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context context )
			throws IOException, InterruptedException{
				
		String line = "";
		if(key.get() == 0){
			return;
		}else{
			String[] tok = value.toString().split(",");
			if(tok[9].contains("\"")){ 
				String[] parts = value.toString().split("\"");
				parts[1] = parts[1].replace(',', ' ');
				for(int i=0;i<parts.length;i++){
					line += parts[i];
				}					
			}else{
				line = value.toString();
			}
			
			String[] tokens = line.split(",");			
			Text redkey = new Text();
			Text redvalue = new Text();
			
			redkey.set(tokens[9]);
			if(tokens[30].isEmpty())tokens[30]="0";
			if(tokens[32].isEmpty())tokens[32]="0";
			String list = tokens[6]+","+tokens[30]+","+tokens[32];
					  // import/export + net_weight + value 	
			redvalue.set(list);
			context.write(redkey, redvalue);
		}			
	}		
}