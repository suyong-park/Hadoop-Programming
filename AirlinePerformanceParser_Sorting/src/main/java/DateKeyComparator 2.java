import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {
    // 복합키 비교기는 복합키의 정렬 순서를 부여하기 위한 클래스이다.

    protected DateKeyComparator() {
        super(DateKey.class, true);
    }

    // compare 메소드는 이미 WritableComparator에 정의되어 있다. 하지만, 객체 스트림으로 비교하기 때문에 부정확하다.
    // 그러므로, compare 메소드를 재정의하도록 한다.
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // 2개의 WritableComparable 객체를 파라미터로 전달받아 DateKey(복합키) 타입으로 형변환한다.
        // 그래야만 DateKey에 선언한 멤버 변수를 조회할 수 있기 때문이다.
        // 복합키 클래스 형변환
        DateKey k1 = (DateKey) w1;
        DateKey k2 = (DateKey) w2;

        // 연도 비교
        // DateKey로 형변환하여 멤버 변수를 조회할 수 있게 됐으므로 연도값을 우선적으로 비교한다.
        int cmp = k1.getYear().compareTo(k2.getYear());
        if(cmp != 0)
            return cmp;
        // 두 값이 동일한 경우 0, k1이 클 경우 1, k1이 작을 경우 -1을 반환한다.
        // 연도가 일치할 경우, 월을 비교해야 한다. 따라서, 다음 줄로 넘어가게 된다. 연도가 일치하지 않는 경우 위 코드에서 정렬되게 된다.

        // 월 비교
        return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2.getMonth() ? -1 : 1);
        // 월을 기준으로 오름차순으로 정렬되게 된다. 월이 같으면 0, k1의 월이 더 작다면 -1, k1의 월이 더 크다면 1을 반환한다.
    }
}
