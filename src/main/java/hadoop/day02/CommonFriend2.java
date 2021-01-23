package hadoop.day02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CommonFriend2 {



    static class SharedFriendMapper02 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] friendPersons = line.split("\t");
            String friend = friendPersons[0];
            String[] persons = friendPersons[1].split(",");
            Arrays.sort(persons); //排序

            //两两配对
            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    context.write(new Text(persons[i] + "-" + persons[j] + ":"), new Text(friend));
                }
            }
        }
    }

    /*
    第二阶段的reduce函数主要完成以下任务
    1.<人-人，list(共同朋友)> 中的“共同好友”进行拼接 最后输出<人-人，两人的所有共同好友>
     */
    static class SharedFriendReducer02 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            Set<String> set = new HashSet<String>();
            for (Text friend : values) {
                if (!set.contains(friend.toString()))
                    set.add(friend.toString());
            }
            for (String friend : set) {
                sb.append(friend.toString()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);

            context.write(key, new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();


        //第二阶段
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(CommonFriend2.class);
        job2.setMapperClass(SharedFriendMapper02.class);
        job2.setReducerClass(SharedFriendReducer02.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("/Users/liuli/output"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/liuli/output1"));

        boolean res2 = job2.waitForCompletion(true);

        System.exit(res2? 0 : 1);

    }

}
