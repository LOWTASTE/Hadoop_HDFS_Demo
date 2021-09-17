import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HDFSTest {
    @Test
    public void testListFiles() throws IOException, InterruptedException,
            URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.10.150:9000"),
                configuration, "hadoop");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/hello/src=http___img2.woyaogexing.com_2019_09_11_97914a66d94a463eacf85469c7ca5d06.gif&refer=http___img2.woyaogexing.gif"),
                true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
        // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        // 3 关闭资源
        fs.close();
    }


    @Test
    public void testCopyFromLocalFile() throws IOException,
            InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.10.150:9000"),
                configuration, "hadoop");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("C:\\Users\\LOW_TASTE\\Pictures\\Camera Roll\\src=http___img2.woyaogexing.com_2019_09_11_97914a66d94a463eacf85469c7ca5d06.gif&refer=http___img2.woyaogexing.gif"), new
                Path("/hello"));
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws IOException,
            InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.10.150:9000"),
                configuration, "hadoop");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new
                        Path("/hello/src=http___img2.woyaogexing.com_2019_09_11_97914a66d94a463eacf85469c7ca5d06.gif&refer=http___img2.woyaogexing.gif"), new Path("E:\\Fox.gif"),
                true);
        // 3 关闭资源
        fs.close();
    }
}
