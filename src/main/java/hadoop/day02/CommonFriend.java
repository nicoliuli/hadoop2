package hadoop.day02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

// 求共同好友，输入文件在 resources/friend.txt
public class CommonFriend {

    public static class FriendMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            String[] personFriends = line.split(":");
            if (personFriends.length != 2) {
                return;
            }
            String person = personFriends[0];
            String[] friends = personFriends[1].split(",");

            for (String friend : friends) {
                ctx.write(new Text(friend), new Text(person));
            }

        }
    }

    public static class FriendReduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text friend : values) {
                sb.append(friend.toString()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            ctx.write(key, new Text(sb.toString()));

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        Job job = Job.getInstance(conf, "common friend");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(FriendMapper.class);
        job.setCombinerClass(FriendReduce.class);
        job.setReducerClass(FriendReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path("/Users/liuli/code/hadoop2/src/main/resources/friend.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/liuli/output"));


        boolean res2 = job.waitForCompletion(true);

        System.exit(res2 ? 0 : 1);

    }

}
