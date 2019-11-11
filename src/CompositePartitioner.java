import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompositePartitioner
   extends Partitioner<CompositeKey, IntWritable> {

    @Override
    public int getPartition(CompositeKey pair,
    		IntWritable val,
                            int numberOfPartitions) {
        // make sure that partitions are non-negative
        return pair.getNaturalKey().hashCode() % numberOfPartitions;
     }
}
