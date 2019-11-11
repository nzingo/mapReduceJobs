import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class cleanReducer extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException,InterruptedException{
		
		
		Text redvalue=new Text();
		String array[]={"0","0","0","0"};
		
		for(Text line : values){
			String[] tokens = line.toString().split(",");
			if(Integer.parseInt(tokens[0])==1){
				array[0]=tokens[1];
				array[1]=tokens[2];
			}else if(Integer.parseInt(tokens[0])==2){
				array[2]=tokens[1];
				array[3]=tokens[2];
			}
		}
		String list = array[0] +","+ array[1] +","+ array[2] +","+ array[3];
		redvalue.set(list);
		context.write(key, redvalue);
					
	}		
}