import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 얼마나 많은 항공기가 출발이 지연되었는지를 계산하는 MapReduce Program
    /*
     mapper의 경우 입력키는 offset이고, 입력값은 항공 운항 통계 데이터(csv)가 될 것이다.
     또한, 출력키는 운항연도, 운항월이고 출력값은 출발 지연 건수가 될 것이다.
     WordCount 예제와 유사한 예제이다.
    */

    private final static IntWritable outputValue = new IntWritable(1); // 출력값
    private Text outputKey = new Text(); // 출력키

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

        outputKey.set(parser.getYear() + "," + parser.getMonth()); // 출력키(운항연도,운항월) 설정

        if(parser.getDepartureDelayTime() > 0) // 출발 지연이 발생한 경우, context.write 호출
            context.write(outputKey, outputValue);
    }
}
