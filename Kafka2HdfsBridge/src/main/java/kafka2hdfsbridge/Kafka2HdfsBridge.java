/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka2hdfsbridge;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka2hdfsbridge.config.AppConfig;
import kafka2hdfsbridge.config.HdfsConfig;
import kafka2hdfsbridge.config.KafkaConfig;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Kafka2HdfsBridge {

    public static void main(String args[]) {
        List               topics = new ArrayList();
        FileSystem         hdfsFS;         //org.apache.hadoop.fs.FileSystem
        //Path               hdfsWorkingDir; //org.apache.hadoop.fs.Path
        Path               hdfsWritePath;  //org.apache.hadoop.fs.Path
        FSDataOutputStream oStream = null;
        KafkaConfig        kafkaConfig = new KafkaConfig();
        HdfsConfig         hdfsConfig  = new HdfsConfig();
        AppConfig          appConfig   = new AppConfig();
        String             kafkaTopicName;
        
        //Kafka Consumer 
        kafkaConfig.setConfiguration(appConfig);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getConfiguration());
        topics.add(kafkaConfig.kafkaTopicName);
        kafkaConsumer.subscribe(topics);
        //HDFS file system
        hdfsConfig.setConfiguration(appConfig);
        
        try {
            hdfsFS = FileSystem.get(URI.create(HdfsConfig.hdfsUri), hdfsConfig.getConfiguration());
            //hdfsWorkingDir = hdfsFS.getWorkingDirectory();
            hdfsWritePath  = new Path(hdfsConfig.hdfsPath);
            oStream=hdfsFS.create(hdfsWritePath);
        
            while (true) {
                System.out.println("In reading loop");
                ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(kafkaConfig.poolMS));
                System.out.println(String.format("Records count: %d",records.count()));
                
                for (Object record : records) {
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", 
                           ((ConsumerRecord) record).topic(), 
                           ((ConsumerRecord) record).partition(), 
                           ((ConsumerRecord) record).value()));
                    oStream.writeChars(String.format("%s\n", ((ConsumerRecord) record).value()));
                    System.out.println(String.format("Writing to HDFS : %s", ((ConsumerRecord) record).value())); 
                }
            }
        
        } catch (IOException ex) {
            Logger.getLogger(Kafka2HdfsBridge.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(-2);
        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());    
        } finally {
            kafkaConsumer.close();
            try {
                oStream.flush();
                oStream.close();
            } catch (IOException ex) {
                Logger.getLogger(Kafka2HdfsBridge.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
