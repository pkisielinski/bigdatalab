/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka2hdfsbridge.config;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author piotr
 */
    public class HdfsConfig {
    static public String hdfsUri="hdfs://hdfs1.lan:8020";  
    public String        hdfsPath;
    Configuration configuration;
    
    public HdfsConfig() {
       configuration = new Configuration();
    }
    
    public void setConfiguration() {
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        hdfsPath = "/users/file.out";
    }
    
    public void setConfiguration(AppConfig appConfig) {
        configuration.set("fs.defaultFS", appConfig.getStringValue("Kafka2HdfsBridge.FS_DEFAULT_FS"));
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        System.setProperty("HADOOP_USER_NAME", appConfig.getStringValue("Kafka2HdfsBridge.HADOOP_USER_NAME"));
        System.setProperty("hadoop.home.dir", appConfig.getStringValue("Kafka2HdfsBridge.HADOOP_HOME_DIR"));
        hdfsPath = appConfig.getStringValue("Kafka2HdfsBridge.HDFS_WORKING_DIR") + "/"
              + appConfig.getStringValue("Kafka2HdfsBridge.HDFS_FILE_NAME");
        hdfsPath = hdfsPath.replace("//", "/");
    }    

    public Configuration getConfiguration() {
        return configuration;
    }    
}
