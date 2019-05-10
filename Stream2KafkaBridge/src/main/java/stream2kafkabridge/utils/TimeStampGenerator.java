/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package stream2kafkabridge.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author piotr
 */
public class TimeStampGenerator {
    public static String getTimeStampString(){    
        Date             date       = new Timestamp(System.currentTimeMillis());  
        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd|HH:mm:ss.SSS");
        String           dateString = dateFormat.format(date);
        
        return dateString;  
    }
}
