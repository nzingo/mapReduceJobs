import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKeyDescend extends CompositeKey
implements Writable, WritableComparable<CompositeKey> {

  private Text naturalKey = new Text();                 // natural key
  private IntWritable secondaryKey = new IntWritable(); // secondary key

  @Override
  /**
  * This comparator controls the sort order of the keys.
  */
  public int compareTo(CompositeKey pair) {
      int compareValue = this.naturalKey.compareTo(pair.getNaturalKey());
      if (compareValue == 0) {
          compareValue = secondaryKey.compareTo(pair.getSecondaryKey());
      }
      return compareValue;    // sort ascending
      // return -1*compareValue;   // sort descending
  }

	public IntWritable getSecondaryKey() {
		return this.secondaryKey;
	}

	public Text getNaturalKey() {
		return this.naturalKey;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		// TODO Auto-generated method stub
		naturalKey.set(WritableUtils.readString(dataInput));
		secondaryKey.set(WritableUtils.readVInt(dataInput));
		
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(dataOutput, naturalKey.toString());
		WritableUtils.writeVInt(dataOutput, secondaryKey.get());
		
	}

	public void setNaturalKey(String ym) {
		this.naturalKey.set(ym);
	}

	public void setSecondaryKey(int t) {
		this.secondaryKey.set(t);
	}
}