import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayMainWithCounter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new DelayMainWithCounter(), args); // run 메소드 호출
        System.out.println("MR-Job Result : " + res);
    }

    @Override
    public int run(String[] args) throws Exception {

        String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        if(otherArgs.length != 2) {
            System.err.println("Usage : DelayMain <in> <out>");
            System.exit(2);
        }

        Job job = new Job(getConf(), "DelayMain");

        job.setJarByClass(DelayMainWithCounter.class);
        job.setMapperClass(DelayMapperWithCounter.class);
        job.setReducerClass(DelayReducerWithCounter.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
        return 0;
    }
}
