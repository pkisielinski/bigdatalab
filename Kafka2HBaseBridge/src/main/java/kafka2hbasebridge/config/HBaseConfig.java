/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka2hbasebridge.config;

import java.util.Properties;

/**
 *
 * @author piotr
 */
public class HBaseConfig {
 
    public Properties  configuration;
    
    public HBaseConfig() {
            configuration = new Properties();
    }
    
    public void setConfiguration() {
            configuration.put("HBase.HBASE_ZOOKEPER_QUORUM", "hbase.lan");
            configuration.put("HBase.HBASE_ZOOKEPER_PROPERTY_CLIENTPORT","2181");
            configuration.put("HBase.HBASE_MASTER", "hbase.home:16010");            
    }

   
    public void setConfiguration(AppConfig appConfig) {
            configuration.put("HBase.HBASE_ZOOKEPER_QUORUM", 
                               appConfig.getStringValue("HBase.HBASE_ZOOKEPER_QUORUM"));
            configuration.put("HBase.HBASE_ZOOKEPER_PROPERTY_CLIENTPORT",
                               appConfig.getStringValue("HBase.HBASE_ZOOKEPER_PROPERTY_CLIENTPORT"));
            configuration.put("HBase.HBASE_MASTER", 
                               appConfig.getStringValue("HBase.HBASE_MASTER"));            
    }        
    
    public Properties getConfiguration() {
        return configuration;
    }
    
    public String getValue(String key) {
        String value = "";
        try {
         value = configuration.getProperty(key);
        } catch(Exception e) {}
        finally {
          return value;
        }
    }

}