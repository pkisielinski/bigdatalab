/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stream2kafkabridge.config;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 *
 * @author piotr
 */
public class KafkaConfig {

    public Properties  configuration;
    KafkaConfig kafkaConfig;
    
    public KafkaConfig() {
        configuration = new Properties();  
    }

    public void setConfiguration() {
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.lan:9092");
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, "Stream2KafkaBridgeProducer");
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
    
    public void setConfiguration(AppConfig appConfig) {
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                          appConfig.getStringValue("ProducerConfig.BOOTSTRAP_SERVERS_CONFIG"));
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, 
                          appConfig.getStringValue("ProducerConfig.GROUP_ID_CONFIG"));
        configuration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configuration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }    
    

    public Properties getConfiguration() {
        return configuration;
    }

}
