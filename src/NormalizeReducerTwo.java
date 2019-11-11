import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NormalizeReducerTwo
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
		 int th_hold = 1;
		 
		 int counter = 1;
		 int valref = 1;
		 
		 for (Text line : values) {
			  float idx;
			  String List = "";
			  String[] tokens = line.toString().split(",");
			  int balance = Integer.parseInt(tokens[2]);
			  if(counter <= th_hold){
					valref = balance;
					idx = 100;
			  }else{
					idx =((float)balance / (float)valref) * 100;
			  }
			  if(idx < 0){
				  idx = 0;
			  }
			  String index = String.format ("%.2f", idx);
			  List = tokens[1]+","+tokens[2]+","+tokens[3]+","+tokens[4]+","+index; // tokens 4 added
			  redkey.set(tokens[0]);
			  redvalue.set(List);
			  context.write(redkey, redvalue);
			  
			  counter++;
		 }
	}
}