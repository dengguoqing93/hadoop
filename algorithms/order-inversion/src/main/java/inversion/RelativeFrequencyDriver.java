package inversion;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 提交作业的驱动器
 *
 * @author dengguoqing
 * @date 2019/2/27
 */
public class RelativeFrequencyDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("Relative frequency statistic");
        job.setJarByClass(RelativeFrequencyDriver.class);

        FileInputFormat.addInputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/order-inversion/src/main/resources/sample_input.txt"));
        FileOutputFormat.setOutputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/order-inversion/src/main/resources/output"));

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setCombinerClass(RelativeFrequencyCombiner.class);
        job.setReducerClass(RelativeFrequencyReducer.class);

        job.setPartitionerClass(OrderInversionPartitioner.class);

        job.setMapOutputKeyClass(PairOfWords.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(PairOfWords.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.waitForCompletion(true);
    }
}
