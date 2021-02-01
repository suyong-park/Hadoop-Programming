import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupKeyComparator extends WritableComparator {
    // 리듀서는 그룹키 비교기를 통해 같은 연도에 해당하는 모든 데이터를 하나의 Reducer 그룹에서 처리할 수 있다.

    protected GroupKeyComparator() {
        super(DateKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DateKey k1 = (DateKey) w1;
        DateKey k2 = (DateKey) w2;

        // 연도값 비교
        return k1.getYear().compareTo(k2.getYear());
    }
}
