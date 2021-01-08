package day01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.net.URI;

public class HelloWord {
    private String host = "hdfs://192.168.43.50:9000";

    @Test
    public void listFiles() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(host), conf, "root");
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("/"),true);
        while (list.hasNext()){
            LocatedFileStatus next = list.next();
            System.out.println(next.getPath().getName());
        }

        fs.close();
    }

    @Test
    public void delete() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(host), conf, "root");
        fs.delete(new Path("/WPS_Office_1.9.1(2994).dmg"));
        fs.close();
    }

    @Test
    public void copyFromLocalFile() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(host), conf, "root");
        fs.copyFromLocalFile(new Path("/Users/liuli/Downloads/soft/WPS_Office_1.9.1(2994).dmg"),new Path("/"));
        fs.close();
    }
    @Test
    public void copyToLocalFile() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(host), conf, "root");
        fs.copyToLocalFile(new Path("/aaa.txt"),new Path("/Users/liuli/aaa.txt"));
        fs.close();
    }
}
