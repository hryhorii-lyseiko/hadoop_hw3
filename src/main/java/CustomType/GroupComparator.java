package CustomType;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(CustomKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CustomKey ck1 = (CustomKey) w1;
        CustomKey ck2 = (CustomKey) w2;
        return CustomKey.compare(ck1.getCityId(), ck2.getCityId());
    }
}
