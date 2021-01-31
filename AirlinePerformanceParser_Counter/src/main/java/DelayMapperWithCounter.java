import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {

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

        if(delay.equals("departure")) {
            if(parser.isDepartureDelayAvailable()) {
                if(parser.getDepartureDelayTime() > 0) {
                    outputKey.set(parser.getYear() + "," + parser.getMonth()); // 출력키(운항연도,운항월) 설정
                    context.write(outputKey, outputValue);
                }
                else if(parser.getDepartureDelayTime() == 0)
                    context.getCounter(DelayCounters.scheduled_departure).increment(1);
                else if(parser.getDepartureDelayTime() < 0)
                    context.getCounter(DelayCounters.early_departure).increment(1);
            }
            else
                context.getCounter(DelayCounters.not_available_departure).increment(1);
        }
        else if(delay.equals("arrival")) {
            if(parser.isArriveDelayAvailable()) {
                if(parser.getArriveDelayTime() > 0) {
                    outputKey.set(parser.getYear() + "," + parser.getMonth()); // 출력키(운항연도,운항월) 설정
                    context.write(outputKey, outputValue);
                }
                else if(parser.getArriveDelayTime() == 0)
                    context.getCounter(DelayCounters.scheduled_arrival).increment(1);
                else if(parser.getArriveDelayTime() < 0)
                    context.getCounter(DelayCounters.early_arrival).increment(1);
            }
            else
                context.getCounter(DelayCounters.not_available_arrival).increment(1);
        }

        else {
            System.err.println("Select 'arrival' or 'departure'");
            System.exit(2);
        }
    }
}
