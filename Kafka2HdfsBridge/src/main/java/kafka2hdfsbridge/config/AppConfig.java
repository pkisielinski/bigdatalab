/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafka2hdfsbridge.config;

import java.util.ResourceBundle;

/**
 *
 * @author piotr
 */
public class AppConfig {
    
    ResourceBundle appConfig;
    
    public AppConfig() {
        appConfig = ResourceBundle.getBundle("AppConfig");
    }
       
    public String getStringValue(String key) {
        return appConfig.getString(key);
    }
            
    
}
