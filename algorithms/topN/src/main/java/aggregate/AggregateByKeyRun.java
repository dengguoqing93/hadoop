package aggregate;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 本地运行聚合
 *
 * @author dengguoqing
 * @date 2019/2/20
 */
public class AggregateByKeyRun {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJarByClass(AggregateByKeyRun.class);
        job.setJobName("Aggregate");

        String input = "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/topN/src/main/resources/file1.txt";
        String output = "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/topN/src/main/resources/final";

        FileInputFormat.addInputPath(job, new Path(input));
        FileInputFormat.addInputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/topN/src/main/resources/file2.txt"));
        FileInputFormat.addInputPath(job,new Path("file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/topN/src/main/resources/file3.txt"));
        FileOutputFormat.setOutputPath(job, new Path(output));


        job.setMapperClass(AggregateByKeyMapper.class);
        job.setReducerClass(AggregateByKeyReducer.class);
        job.setCombinerClass(AggregateByKeyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
