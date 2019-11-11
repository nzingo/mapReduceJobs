import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NormalizeReducerOne
	extends Reducer<CompositeKey,Text,Text,Text> {
	
	public void reduce(CompositeKey key, Iterable<Text> values,
	                Context context
	                ) throws IOException, InterruptedException {
	
		Text redkey=new Text();
		Text redvalue=new Text();
		 
		 /*
		 int size = 0;
		 for (Text line : values) {
				size++;
		 }
		 int th_hold = size/10;
		 */
		 int th_hold = 11;
		 
		 int counter = 1;
		 int valref = 1;
		 
		 for (Text line : values) {
			  float idx;
			  String List = "";
			  String[] tokens = line.toString().split(",");
			  int import_value = Integer.parseInt(tokens[1]);
			  if(counter <= th_hold){
					valref = import_value;
					idx = 100;
			  }else{
					idx =((float)import_value / (float)valref) * 100;
			  }
			  String index = String.format ("%.2f", idx);
			  List = tokens[1]+","+tokens[2]+","+tokens[3]+","+index;
			  redkey.set(tokens[0]);
			  redvalue.set(List);
			  context.write(redkey, redvalue);
			  
			  counter++;
		 }
	}
}