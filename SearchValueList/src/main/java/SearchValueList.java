import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SearchValueList extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SearchValueList(), args);
        System.out.println("MR-Jobs Result : " + res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Path path = new Path(args[0]);
        FileSystem fs = path.getFileSystem(getConf());

        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());
        // 맵파일 조회

        IntWritable key = new IntWritable();
        key.set(Integer.parseInt(args[1]));
        // 검색 키를 저장할 객체를 선언한다. 사용자가 입력한 운항 거리를 선언하는 것이다. 맵파일의 키가 운항 거리이기 때문이다.

        Text value = new Text();
        // 검색 값을 저장할 객체를 선언한다.

        Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
        MapFile.Reader reader = readers[partitioner.getPartition(key, value, readers.length)];
        // 파티셔너를 이용해 검색 키가 저장된 맵 파일을 조회한다.
        // 이전 단계에서 맵 파일이 해시 파티셔너로 파티셔닝됐기 때문에 해시 파티셔너를 사용한다.
        // getPartition 메소드는 특정 키에 대한 파티션 번호를 반환하게 된다.

        Writable entry = reader.get(key, value);
        // get 메소드를 사용하여 특정 키에 해당되는 값을 검색한다. 이 때, 검색되는 값은 첫 번째 값이다.
        if(entry == null)
            System.out.println("The requested key was not found.");
        // 검색 결과 확인
        // 첫 번째 값이 존재하지 않는다면 키에 해당되는 값이 없는 것이므로 위와 같이 출력한다.

        IntWritable nextKey = new IntWritable();
        do {
            System.out.println(value.toString());
        } while (reader.next(nextKey, value) && key.equals(nextKey));
        // 맵 파일을 순회하며 키와 값을 출력한다.
        // 검색된 값이 존재한다면 do/while문으로 순회하면서 맵 파일에 있는 모든 데이터를 조회하게 된다.
        // 이 때, next 메소드는 다음 순서의 데이터로 위치를 이동하고, key와 value의 파라미터에 현재 위치의 값을 설정하게 된다.

        return 0;
    }
}
