import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private String delay;
    private final static IntWritable outputValue = new IntWritable(1);
    private Text outputKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        delay = context.getConfiguration().get("workType");
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

        if(delay.equals("arrival")) {

            outputKey.set(parser.getYear() + "," + parser.getMonth());
            if(parser.getArriveDelayTime() > 0)
                context.write(outputKey, outputValue);
        }
        else if(delay.equals("departure")) {

            outputKey.set(parser.getYear() + "," + parser.getMonth()); // 출력키(운항연도,운항월) 설정
            if(parser.getDepartureDelayTime() > 0) // 출발 지연이 발생한 경우, context.write 호출
                context.write(outputKey, outputValue);
        }
        else {
            System.err.println("Select 'arrival' or 'departure'");
            System.exit(2);
        }
    }
}
