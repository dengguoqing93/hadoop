package job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 共同好友驱动器
 *
 * @author dengguoqing
 * @date 2019/3/2
 */
public class CommonFriendsDriver {
    public static void main(String[] args) throws Exception {
        String input1 = "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/common-friend/src/main/resources/file1.txt";
        String input2 = "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/common-friend/src/main/resources/file2.txt";
        String output = "file:///Users/dengguoqing/IdeaProjects/hadoop/algorithms/common-friend/src/main/resources/output";

        Job job = Job.getInstance();
        job.setJobName("共同好友");
        job.setJarByClass(CommonFriendsDriver.class);

        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(CommonFriendsMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
