import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ArrivalDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable outputValue = new IntWritable(1);
    private Text outputKey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

        outputKey.set(parser.getYear() + "," + parser.getMonth());
        if(parser.getArriveDelayTime() > 0)
            context.write(outputKey, outputValue);
    }
}
