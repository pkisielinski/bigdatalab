/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka2hbasebridge.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 *
 * @author piotr
 */
public class KafkaConfig {

    public Properties  configuration;
    KafkaConfig        kafkaConfig;
    public String      kafkaTopicName;
    public int         poolMS;
    
    public KafkaConfig() {
        configuration = new Properties();
    }
 
    public void setConfiguration() {
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.lan:9092");
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, "Kafka2HBaseBridgeConsumer");
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        configuration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "2000");
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                          StringDeserializer.class.getName());
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                          StringDeserializer.class.getName());
        kafkaTopicName = "txns-topic";
        poolMS         = 2000;
    }

   
    public void setConfiguration(AppConfig appConfig) {
        configuration.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                          appConfig.getStringValue("ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG"));
        configuration.put(ConsumerConfig.GROUP_ID_CONFIG, 
                          appConfig.getStringValue("ConsumerConfig.GROUP_ID_CONFIG"));
        configuration.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, 
                          appConfig.getStringValue("ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG"));
        configuration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 
                          appConfig.getStringValue("ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG"));
        configuration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configuration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaTopicName = appConfig.getStringValue("ConsumerConfig.TOPIC_NAME");
        poolMS         = Integer.parseInt(appConfig.getStringValue("ConsumerConfig.POOL_MS"));
    }        
    
    public Properties getConfiguration() {
        return configuration;
    } 

}
