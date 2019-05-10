package kafka2hbasebridge;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka2hbasebridge.config.AppConfig;
import kafka2hbasebridge.config.HBaseConfig;
import kafka2hbasebridge.config.KafkaConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Kafka2HBaseBridge {

    public static void main(String args[]) {
        AppConfig appConfig = new AppConfig();
        List topics = new ArrayList();
        KafkaConfig kafkaConfig = new KafkaConfig();
        String kafkaTopicName = null;
        HBaseConfig hBaseConfig = new HBaseConfig();
        Connection hBaseConn;
        Admin hBaseAdmin;
        Table hTable;
        Put hBaseRecord;
        String key = null;
        String value = null;
        String fields[];
        final String tableName = "transactions";
        final String familyTransaction = "transaction";
        final String familyCard = "card";

        try {
            //Kafka Consumer
            kafkaConfig.setConfiguration(appConfig);
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getConfiguration());
            topics.add(kafkaConfig.kafkaTopicName);
            kafkaConsumer.subscribe(topics);
            //HBase
            hBaseConfig.setConfiguration(appConfig);
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", hBaseConfig.getValue("HBase.HBASE_ZOOKEPER_QUORUM"));
            config.set("hbase.zookeeper.property.clientPort", hBaseConfig.getValue("HBase.HBASE_ZOOKEPER_PROPERTY_CLIENTPORT"));
            config.set("hbase.master", hBaseConfig.getValue("HBase.HBASE_MASTER"));
            //Test connection
            HBaseAdmin.available(config);
            //Connect to HBase

            hBaseConn = ConnectionFactory.createConnection(config);
            hBaseAdmin = hBaseConn.getAdmin();
            hTable = hBaseConn.getTable(TableName.valueOf(tableName));

            int i = 0;
            while (i < 100) {
                System.out.println(String.format("Starting reading loop : %d", i++));
                ConsumerRecords records = kafkaConsumer.poll(Duration.ofMillis(kafkaConfig.poolMS));
                System.out.println(String.format("Records count: %d", records.count()));

                for (Object kafkaRecord : records) {
                    System.out.println(String.format("Topic - %s, Offset - %d, Partition - %d, Value: %s",
                            ((ConsumerRecord) kafkaRecord).topic(),
                            ((ConsumerRecord) kafkaRecord).offset(),
                            ((ConsumerRecord) kafkaRecord).partition(),
                            ((ConsumerRecord) kafkaRecord).value()));
                    value = String.format("%s", ((ConsumerRecord) kafkaRecord).value());
                    fields = value.split("\\|");
                    key = generateKey(value);

                    hBaseRecord = new Put(Bytes.toBytes(key));

                    hBaseRecord.addColumn(Bytes.toBytes(familyTransaction), Bytes.toBytes("date"), Bytes.toBytes(fields[0]));
                    hBaseRecord.addColumn(Bytes.toBytes(familyTransaction), Bytes.toBytes("time"), Bytes.toBytes(fields[1]));
                    hBaseRecord.addColumn(Bytes.toBytes(familyTransaction), Bytes.toBytes("value"), Bytes.toBytes(fields[5]));
                    hBaseRecord.addColumn(Bytes.toBytes(familyTransaction), Bytes.toBytes("zip"), Bytes.toBytes(fields[4]));
                    hBaseRecord.addColumn(Bytes.toBytes(familyCard), Bytes.toBytes("vendor"), Bytes.toBytes(fields[2]));
                    hBaseRecord.addColumn(Bytes.toBytes(familyCard), Bytes.toBytes("pan"), Bytes.toBytes(fields[3]));

                    hTable.put(hBaseRecord);
                }

            }
            //End Connection
            hTable.close();
            hBaseConn.close();
        } catch (MasterNotRunningException ex) {
            Logger.getLogger(Kafka2HBaseBridge.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ZooKeeperConnectionException ex) {
            Logger.getLogger(Kafka2HBaseBridge.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Kafka2HBaseBridge.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(Kafka2HBaseBridge.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static String generateKey(String value) {
        return DigestUtils.md5Hex(value).toUpperCase();
    }

}
