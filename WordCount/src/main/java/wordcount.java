import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class wordcount {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 입력 파일이 다수의 input split으로 쪼개지고, 각 input split 내부에서 각각의 records 마다 map 함수가 한 번씩 적용된다.

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            // 공백으로 단어를 분리하기 위해 StringTokenizer을 사용한다. 이 때, 추가적으로 조건을 달아줌으로써 해당 조건을 사용하여 단어를 분리할 수도 있다.

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable value = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // reduce는 map task의 중간 결과물을 받게 된다. 이 때 key값은 단어, values값은 1일 것이다.

            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            value.set(sum);
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // 하둡 환경설정 파일에 접근하기 위한 클래스이다.

        Job job = new Job(conf, "wordcount");
        // 맵리듀스 job을 실행하기 위한 job 객체를 생성한다.

        job.setJarByClass(wordcount.class);
        // 사용할 main 클래스를 말한다.

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        // mapper, reducer 클래스를 결정한다.

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Text ~ Format은 \n을 기준으로 레코드를 분류하며, Key는 offset, LongWritable 타입을 사용한다. Value는 라인의 내용이며, Text 타입을 사용한다.

        job.setNumReduceTasks(1);
        // Reduce Task는 1개를 구동한다.

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}