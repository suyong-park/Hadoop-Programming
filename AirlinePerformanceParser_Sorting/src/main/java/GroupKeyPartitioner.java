import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupKeyPartitioner extends Partitioner<DateKey, IntWritable> {
    // 여기서, Partitioner에 있는 2개의 파라미터 값은 Mapper의 출력 데이터의 키와 값에 해당하는 파라미터이다.

    @Override
    public int getPartition(DateKey key, IntWritable val, int numPartitions) {
        // 파티셔너는 getPartition 메소드를 호출해 파티셔닝 번호를 조회하게 된다.
        // 연도가 키값이 되므로 연도에 대한 해시값을 조회해 파티션 번호를 생성하도록 한다.

        int hash = key.getYear().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
}
