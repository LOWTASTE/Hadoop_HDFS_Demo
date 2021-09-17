package Demo1;//package Demo1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Application {
    public static void main(String[] args) {
        try {
//  配置连接地址
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.10.150:9000");
//  conf.set("dfs.client.use.datanode.hostname", "true");
            FileSystem fs = FileSystem.get(conf);
//  打开文件并读取输出
            Path hello = new Path("/hello/hello.txt");
            FSDataInputStream ins = fs.open(hello);
            int ch = ins.read();
            while (ch != -1) {
                System.out.print((char)ch);
                ch = ins.read();
            }
            System.out.println();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
