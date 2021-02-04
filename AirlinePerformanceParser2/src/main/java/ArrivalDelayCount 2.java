import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ArrivalDelayCount {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        System.out.println(args.length);
        if(args.length != 2) {
            System.err.println("Usage : ArrivalDelayCount <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "ArrivalDelayCount");

        job.setJarByClass(ArrivalDelayCount.class);
        job.setMapperClass(ArrivalDelayCountMapper.class);
        job.setReducerClass(DelayCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}
