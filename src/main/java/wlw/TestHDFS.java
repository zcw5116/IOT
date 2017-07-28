package wlw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * Created by zhoucw on 17-7-18.
 */
public class TestHDFS {

    /**
     * @param hdfs FileSystem 对象
     * @param path 文件路径
     */
    public static void iteratorShowFilesWithDir(FileSystem hdfs, Path path) {
        try {
            if (hdfs == null || path == null) {
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);

            //展示文件信息
            for (int i = 0; i < files.length; i++) {
                try {
                    if (files[i].isDirectory()) {
                        System.out.println(">>>" + files[i].getPath()
                                + ", dir owner:" + files[i].getOwner());
                        //递归调用
                        iteratorShowFiles(hdfs, files[i].getPath());
                    } else if (files[i].isFile()) {
                        System.out.println("   " + files[i].getPath()
                                + ", length:" + files[i].getLen()
                                + ", owner:" + files[i].getOwner());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param hdfs FileSystem 对象
     * @param path 文件路径
     */
    public static void iteratorShowFiles(FileSystem hdfs, Path path) {
        try {
            if (hdfs == null || path == null) {
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);


            Long filesize = 0l;
            //展示文件信息
            for (int i = 0; i < files.length; i++) {
                try {
                    if (files[i].isFile()) {
                        filesize = filesize + files[i].getLen();
                        System.out.println("   " + files[i].getPath()
                                + ", length:" + files[i].getLen()
                                + ", owner:" + files[i].getOwner());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            double msize = (double)filesize/1024/1024;
            double numsize = Math.ceil(msize);

            System.out.println("numsize:" + numsize);
            System.out.println("filesize:" + filesize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        FileSystem hdfs = null;
        FileSystem filesystem = null;
        try{
            Configuration config = new Configuration();
            // 程序配置
            config.set("fs.default.name", "hdfs://cdh-nn1:8020");
            //config.set("hadoop.job.ugi", "feng,111111");
            //config.set("hadoop.tmp.dir", "/tmp/hadoop-fengClient");
            //config.set("dfs.replication", "1");
            //config.set("mapred.job.tracker", "master:9001");
            hdfs = FileSystem.get(new URI("hdfs://cdh-nn1:8020"),
                    config);
            Path path = new Path("/hadoop/zcw/tmp/wcout");
            // 获取要读取的文件的根目录的所有二级子文件目录

            FileStatus[] statuses = filesystem.listStatus(new Path("/schemas_folder"), new PathFilter()
            {
                private final Pattern pattern = Pattern.compile("^date=[0-9]{8}$");

                @Override
                public boolean accept(Path path)
                {
                    return pattern.matcher(path.getName()).matches();
                }
            });

            long asize = hdfs.getContentSummary(path).getLength();
            System.out.println("asize:" + asize);
            iteratorShowFiles(hdfs, path);

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(hdfs != null){
                try {
                    hdfs.closeAll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }




    }


}
