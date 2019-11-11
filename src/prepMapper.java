import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class prepMapper 
	extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context context )
			throws IOException, InterruptedException{
		
		String[] tokens = value.toString().split(",");
		
		Text textKey = new Text();
		Text textValue = new Text();
		
		String Reporter = tokens[0];
		String col1 = tokens[1];
		String col2 = tokens[2];
		// String col3 = tokens[3];
		String col4 = tokens[4];
		
		int import_Netweight = Integer.parseInt(col1);
		int import_value = Integer.parseInt(col2);
		// int export_netweight = Integer.parseInt(col3);
		int export_value = Integer.parseInt(col4);
		
		int balance_commerciale = export_value - import_value;
		float valeur_unitaire = 0;
		if(import_Netweight != 0){
			valeur_unitaire = (float)import_value / (float)import_Netweight;
		}
					
		String v = Integer.toString(import_value) + ',' + 
			   	   Integer.toString(balance_commerciale) + ',' +
				   Float.toString(valeur_unitaire);
		
		textKey.set(Reporter);
		textValue.set(v);
		
		context.write(textKey,textValue);
					
	}		
}