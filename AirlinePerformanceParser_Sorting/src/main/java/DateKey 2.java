import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateKey implements WritableComparable<DateKey> {

    private String year;
    private Integer month;

    public DateKey() {
    }

    public DateKey(String year, Integer month) {
        this.year = year;
        this.month = month;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    @Override
    public String toString() { // toString 메소드를 재정의하여 출력을 어떤 식으로 할지 결정한다. 정의하지 않는다면 자바의 toString이 호출되게 된다.
        return (new StringBuilder()).append(year).append(",").append(month).toString();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException { // 입력 스트림에서 연도와 월을 조회함
        year = WritableUtils.readString(dataInput);
        month = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException { // 출력 스트림에 연도와 월을 출력함
        WritableUtils.writeString(dataOutput, year);
        dataOutput.writeInt(month);
    }

    @Override
    public int compareTo(DateKey key) { // 복합키와 복합키를 비교해 순서를 정할 때 사용한다.
        int result = year.compareTo(key.year);
        if(0 == result) // 연도가 동일한 경우
            result = month.compareTo(key.month); // 연도가 동일할 경우 월을 비교하도록 한다.
        return result;
    }

    // 스트림에서 데이터를 읽고, 출력하는 작업에는 하둡에서 제공하는 WritableUtils를 이용하도록 한다.
}
