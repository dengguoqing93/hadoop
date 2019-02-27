package job;

import average.TimeSeriesData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//

/**
 * MapReduce job for moving averages of time series data
 * by using in memory sort (without secondary sort).
 *
 * @author Mahmoud Parsian
 */
public class SortInMemory_MovingAverageDriver {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(SortInMemory_MovingAverageDriver.class);
        job.setJobName("移动平均");
        // set mapper/reducer
        job.setMapperClass(SortInMemory_MovingAverageMapper.class);
        job.setReducerClass(SortInMemory_MovingAverageReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TimeSeriesData.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/moving-average/src/main/resources/sample.txt"));
        FileOutputFormat.setOutputPath(job, new Path(
                "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/moving-average/src/main/resources/output"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
