import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class NormalizeReducerThree
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
		 int th_hold = 10;
		 
		 int counter = 1;
		 float valref = 1;
		 
		 for (Text line : values) {
			  float idx;
			  String List = "";
			  String[] tokens = line.toString().split(",");
			  float unit_value = Float.parseFloat(tokens[3]);
			  if(counter <= th_hold){
					valref = unit_value;
					idx = 100;
			  }else{
					idx =(unit_value / valref) * 100;
			  }
			  String index = String.format ("%.2f", idx);
			  List = tokens[1]+","+tokens[2]+","+tokens[3]+","+tokens[4]+","+tokens[5]+","+index;
			  redkey.set(tokens[0]);
			  redvalue.set(List);
			  context.write(redkey, redvalue);
			  
			  counter++;
		 }
	}
}