import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DelayCountReducerWithMultipleOutputs extends Reducer<Text, IntWritable, Text, IntWritable> {
    // 항공 출발 지연 데이터와 도착 지연 데이터를 구분하여 aggregation 해야 한다.

    private MultipleOutputs<Text, IntWritable> mos;

    private Text outputKey = new Text(); // reduce 출력키
    private IntWritable result = new IntWritable(); // reduce 출력 값

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<Text, IntWritable>(context);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String[] colums = key.toString().split(","); // , 구분자 관리

        outputKey.set(colums[1] + "," + colums[2]);

        if(colums[0].equals("D")) { // 출발 지연 데이터인 경우
            int sum = 0;
            for(IntWritable value : values)
                sum += value.get();
            result.set(sum);

            mos.write("departure", outputKey, result);
        }
        else { // 도착 지연 데이터인 경우
            int sum = 0;
            for(IntWritable value : values)
                sum += value.get();
            result.set(sum);

            mos.write("arrival", outputKey, result);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }
}
