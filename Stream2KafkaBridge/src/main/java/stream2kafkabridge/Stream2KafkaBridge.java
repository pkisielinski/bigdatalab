package stream2kafkabridge;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import stream2kafkabridge.config.KafkaConfig;
import stream2kafkabridge.config.AppConfig;
import stream2kafkabridge.utils.TimeStampGenerator;

/**
 *
 * @author Piotr
 */
public class Stream2KafkaBridge {
    
     @SuppressWarnings("SleepWhileInLoop")
     public static void main(String args[]) {
        KafkaConfig                   kafkaConfig = new KafkaConfig();
        AppConfig                     appConfig   = new AppConfig();
        KafkaProducer<String, String> kafkaProducer;
        BufferedReader                instream;
        String                        line;
        String                        fileDir;
        String                        fileName;
        String                        filePath;
        String                        kafkaTopicName;
        Long                          interval;
        RecordMetadata                metadata;
        
        kafkaConfig.setConfiguration(appConfig);
        kafkaProducer = new KafkaProducer<>(kafkaConfig.getConfiguration());
        
        fileDir        = appConfig.getStringValue("Stream2KafkaBridge.WORKING_DIR");
        fileName       = appConfig.getStringValue("Stream2KafkaBridge.FILE_NAME");
        filePath       = (fileDir + "/" + fileName).replace("//", "/");
        kafkaTopicName = appConfig.getStringValue("Stream2KafkaBridge.KAFKA_TOPIC_NAME");
        interval       = Long.parseLong(appConfig.getStringValue("Stream2KafkaBridge.SLEEP_MS"));
            
        try {
          instream = new BufferedReader(new FileReader(filePath));  
          for (int i = 0 ; i < 5000; i++) {
              line = instream.readLine();
              if(line != null) {
                    String keyString   = TimeStampGenerator.getTimeStampString();
                    String valueString = keyString + "|" + line;
                  
                    final ProducerRecord<String, String> record =
                            new ProducerRecord<>(kafkaTopicName, keyString, valueString);
                   
                    metadata = kafkaProducer.send(record).get();

                    Thread.sleep(interval);

                    System.out.printf("*\nsent record(key=%s value=%s)\n meta(partition=%d, offset=%d)\n i=%d\n",
                                       record.key(), record.value(), metadata.partition(), metadata.offset(), i);
              }
          }
          instream.close();
        }
        catch (FileNotFoundException ex) {
             Logger.getLogger(Stream2KafkaBridge.class.getName()).log(Level.SEVERE, null, ex);
             System.exit(-1);
        }     
        catch( Exception e ) {
             Logger.getLogger(Stream2KafkaBridge.class.getName()).log(Level.SEVERE, null, e);
             System.exit(-2);
        }    
        finally {
             kafkaProducer.flush();
             kafkaProducer.close();
        }
    }  
    
}
