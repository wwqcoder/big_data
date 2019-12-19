package cn.wwq.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsDemo {

    /**
     * 小文件合并
     */
    //@Test
    public void mergeFile() throws  Exception {
        //获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        FSDataOutputStream outputStream = fileSystem.create(new Path("/bigfile.xml"));

        //获取本地文件系统
        //LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //通过本地文件系统获取文件列表，为一个集合
        //FileStatus[] fileStatuses = local.listStatus(new Path("file:///F:\\big\\data\\上传小文件合并"));


        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hello/mydir/test"));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();

    }

    /**
     * Hdfs 文件上传
     */

    //@Test
    public void putData() throws  Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        fileSystem.copyFromLocalFile(new Path("/Users/wangweiqi/package.json"),new Path("/hello/mydir/test"));
        fileSystem.close();
    }

    /**
     * HDFS上创建文件夹
     */

    //@Test
    public void mkdirs() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        boolean mkdirs = fileSystem.mkdirs(new Path("/hello/mydir/test"));
        fileSystem.close();
    }

    /**
     * 下载文件到本地
     */

    //@Test
    public void getFileToLocal() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");

        FSDataInputStream open = fileSystem.open(new Path("/a.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/wangweiqi/a.txt"));
        IOUtils.copy(open, fileOutputStream);
        IOUtils.closeQuietly(open);
        IOUtils.closeQuietly(fileOutputStream);
        fileSystem.close();

    }

    /**
     * 遍历HDFS中的所有文件[使用API遍历，不包括文件夹]
     */
    //@Test
    public void listMyfiles() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //获取RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            System.out.println(next.getPath().toString());
        }
        fileSystem.close();
    }


    /**
     * 遍历HDFS中的所有文件[包括文件夹]
     */

    //@Test
    public void listFile() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("文件路径为："+fileStatus.getPath().toString());
        }
    }

    /**
     * 获取文件系统的第四种方式
     * @throws URISyntaxException
     * @throws IOException
     */
    //@Test
    public void getFileSystemFour() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem.toString());
    }



    /**
     * 获取文件系统的第三种方式
     * @throws URISyntaxException
     * @throws IOException
     */
   // @Test
    public void getFileSystemThree() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020"), configuration);
        System.out.println(fileSystem.toString());
    }

    /**
     * 获取文件系统的第二种方式
     * @throws URISyntaxException
     * @throws IOException
     */
    //@Test
    public void getFileSystemTwo() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(new URI("/"), configuration);
        System.out.println(fileSystem.toString());
    }

    /**
     * 获取文件系统的第一种方式
     * @throws URISyntaxException
     * @throws IOException
     */
    //@Test
    public void getFileSystemOne() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        System.out.println(fileSystem.toString());
    }
}
