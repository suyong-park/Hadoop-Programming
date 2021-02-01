import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {
    // 복합키 비교기는 복합키의 정렬 순서를 부여하기 위한 클래스이다.

    protected DateKeyComparator() {
        super(DateKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // 복합키 클래스 형변환
        DateKey k1 = (DateKey) w1;
        DateKey k2 = (DateKey) w2;

        // 연도 비교
        int cmp = k1.getYear().compareTo(k2.getYear());
        if(cmp != 0)
            return cmp;
        // 두 값이 동일한 경우 0, k1이 클 경우 1, k1이 작을 경우 -1을 반환한다.

        // 월 비교
        return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
    }
}
