import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritableComparable implements WritableComparable {
    // 해당 코드는 WritableComparable Interface를 상속받아서 새로운 데이터 타입을 만든 것이다.
    // 이 경우, int 타입과 long 타입을 동시에 갖고 있는 데이터 타입이다.

    private int counter;
    private long timestamp;

    public void write(DataOutput out) throws IOException {
        out.writeInt(counter);
        out.writeLong(timestamp);
    }

    public void readFields(DataInput in) throws IOException {
        counter = in.readInt();
        timestamp = in.readLong();
    }

    @Override
    public int compareTo(Object o) {
        MyWritableComparable w = (MyWritableComparable) o;

        if(counter > w.counter)
            return -1;
        else if(counter < w.counter)
            return 1;
        else {
            if (timestamp < w.timestamp)
                return 1;
            else if(timestamp > w.timestamp)
                return -1;
            else
                return 0;
        }
    }
}
