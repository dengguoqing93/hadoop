package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 本地测试运行
 *
 * @author dengguoqing
 * @date 2019/2/16
 */
public class SecondaryRun {
    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            args = new String[]{
                    "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/sorted/src/main/resources/sample_input.txt", "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/sorted/src/main/resources/output"
            };
        }

        Job job = Job.getInstance();
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SecondarySortDriver");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(NaturalValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setMapperClass(SecondaryMapper.class);
        job.setReducerClass(SecondaryReducer.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        //此类决定数据的分组
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

        job.waitForCompletion(true);
    }
}
