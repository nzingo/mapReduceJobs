import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeGroupingComparator
   extends WritableComparator {

    public CompositeGroupingComparator() {
        super(CompositeKey.class, true);
    }

    @SuppressWarnings("rawtypes")
	@Override
    /**
     * This comparator controls which keys are grouped
     * together into a single call to the reduce() method
     */
    public int compare(WritableComparable wc1, WritableComparable wc2) {
    	CompositeKey pair = (CompositeKey) wc1;
    	CompositeKey pair2 = (CompositeKey) wc2;
        return pair.getNaturalKey().compareTo(pair2.getNaturalKey());
    }
}