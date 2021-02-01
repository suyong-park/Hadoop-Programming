import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {

    private MultipleOutputs<DateKey, IntWritable> mos;

    private DateKey outputKey = new DateKey(); // reduce 출력키
    private IntWritable result = new IntWritable(); // reduce 출력 값

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<DateKey, IntWritable>(context);
    }

    public void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String[] colums = key.getYear().split(","); // , 구분자 관리
        int sum = 0;
        Integer bMonth = key.getMonth();

        if(colums[0].equals("D")) { // 출발 지연 데이터인 경우
            for(IntWritable value : values) {
                if (bMonth != key.getMonth()) {
                    result.set(sum);
                    outputKey.setYear(key.getYear().substring(2));
                    outputKey.setMonth(bMonth);
                    mos.write("departure", outputKey, result);
                    sum = 0;
                }
                sum += value.get();
                bMonth = key.getMonth();
            }
            if(key.getMonth() == bMonth) {
                outputKey.setYear(key.getYear().substring(2));
                outputKey.setMonth(key.getMonth());
                result.set(sum);
                mos.write("departure", outputKey, result);
            }
        }
        else { // 도착 지연 데이터인 경우
            for(IntWritable value : values) {
                if(bMonth != key.getMonth()) {
                    result.set(sum);
                    outputKey.setYear(key.getYear().substring(2));
                    outputKey.setMonth(bMonth);
                    mos.write("arrival", outputKey, result);
                    sum = 0;
                }
                sum += value.get();
                bMonth = key.getMonth();
            }
            if(key.getMonth() == bMonth) {
                outputKey.setYear(key.getYear().substring(2));
                outputKey.setMonth(key.getMonth());
                result.set(sum);
                mos.write("arrival", outputKey, result);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }
}

