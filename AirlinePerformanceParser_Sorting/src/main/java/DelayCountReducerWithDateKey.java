import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {
    // 복합키를 입력데이터의 키와 출력 데이터의 키로 사용해야 하므로 위처럼 파라미터값을 설정한다.

    private MultipleOutputs<DateKey, IntWritable> mos;

    private DateKey outputKey = new DateKey(); // reduce 출력키
    private IntWritable result = new IntWritable(); // reduce 출력 값

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs<DateKey, IntWritable>(context);
    }

    public void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        String[] colums = key.getYear().split(","); // , 구분자 분리
        int sum = 0;
        Integer bMonth = key.getMonth();

        if(colums[0].equals("D")) { // 출발 지연 데이터인 경우
            for(IntWritable value : values) {
                if (bMonth != key.getMonth()) {
                    // 이렇게 bMonth 변수를 생성해서 월 값을 백업해 두지 않게 되는 경우 12월만 출력되게 되고, 지연 횟수는 해당 연도의 전체 지연 횟수가 출력된다.
                    // 따라서, 월값을 bMonth 변수에 백업해 두어 월별 지연 횟수가 출력될 수 있게끔 코드를 수정한다.
                    result.set(sum);
                    outputKey.setYear(key.getYear().substring(2));
                    outputKey.setMonth(bMonth);
                    mos.write("departure", outputKey, result);
                    sum = 0;
                    // 다음 월의 지연 횟수를 합산할 수 있도록 0으로 초기화한다.
                }
                sum += value.get();
                bMonth = key.getMonth();
            }
            if(key.getMonth() == bMonth) {
                // 그 다음 Iterable 객체의 순회가 종료되고 나면 최종적으로 월의 지연 횟수를 출력하도록 한다. 이렇게 해야 12월까지 출력된다.
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

