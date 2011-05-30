package com.avrolog.log4j.layout;

import static com.avrolog.log4j.layout.LogConstants.BINARY_TYPE;
import static com.avrolog.log4j.layout.LogConstants.LEVEL;
import static com.avrolog.log4j.layout.LogConstants.LOGGER;
import static com.avrolog.log4j.layout.LogConstants.MESSAGE;
import static com.avrolog.log4j.layout.LogConstants.NDC;
import static com.avrolog.log4j.layout.LogConstants.THREADNAME;
import static com.avrolog.log4j.layout.LogConstants.THROWABLE;
import static com.avrolog.log4j.layout.LogConstants.TIMESTAMP;
import static com.avrolog.log4j.layout.LogConstants.MDC;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;


/****
 * A layout for log4j using avro
 * 
 * @author harisgx
 *
 */
public class AvroLogLayout extends Layout {
	

	private String type;// binary or json? loaded from log4j.properties
	private List<String> mdcKeys = new ArrayList<String>();//mdc keys, user has to set these values
	
	public AvroLogLayout(){
		
	}	
	
	@Override
	public String format(LoggingEvent event) {
		ByteArrayOutputStream bao = null;
		String logOutput = "";		
		try{	
				
			Schema avroSchema = SchemaUtil.instance().getSchema();
			if(avroSchema != null){
				GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);
				Encoder encoder = null;
				bao = new ByteArrayOutputStream();
				
				encoder = EncoderFactory.get().jsonEncoder(avroSchema, bao);
							
				GenericRecord record = new GenericData.Record(avroSchema);
				putBasicFields(event, record);
				putNDCValues(event, record);
				putThrowableEvents(event, record);	
				putMDCValues(event, record);							
				writer.write(record, encoder);				
				encoder.flush();
				logOutput = new String(bao.toByteArray());
			}			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(bao != null){
				try {
					bao.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return logOutput + "\n";
	}
	
	
	private void putBasicFields(LoggingEvent event, GenericRecord record){
		record.put(LOGGER, event.getLoggerName());
		record.put(LEVEL, event.getLevel().toString());
		record.put(TIMESTAMP,event.getTimeStamp());
		record.put(THREADNAME, event.getThreadName());			    
		record.put(MESSAGE, new Utf8(event.getMessage().toString()));
	}
	
	private void putNDCValues(LoggingEvent event, GenericRecord record){
		  if (event.getNDC() != null) {
			  record.put(NDC, event.getNDC());
	      }
	}
	
	private void putThrowableEvents(LoggingEvent event, GenericRecord record){
		String throwableEvents = getThrowableEvents(event);
		 if(throwableEvents.length() > 0){
			 record.put(THROWABLE, throwableEvents);	
		 }
	}
	
	private void putMDCValues(LoggingEvent event, GenericRecord record){
		
		Map<String, String> mdcMap = getMDCValues(event);
		if(mdcMap != null){
			record.put(MDC, mdcMap);
		}	
	}
    	
	private String getThrowableEvents(LoggingEvent event) {
       
		StringBuilder throwableStr = new StringBuilder();
        String[] throwableStrRep = event.getThrowableStrRep();
        //throwable event will be a line seperated string -- stacktrace?
        if (throwableStrRep != null) {
            for (String str : throwableStrRep) {
            	throwableStr.append(str).append("\n");
            }
        }
       
       return throwableStr.toString();
        
    }
	
	/***
	 * 
	 * @param event
	 * @return
	 */
	private Map<String, String> getMDCValues(LoggingEvent event){
		Map<String, String> hashMap = null;
		if (mdcKeys != null && mdcKeys.size() > 0) {
			//Obtain a copy of MDC prior to serialization or asynchronous logging.
            event.getMDCCopy();
            hashMap = new HashMap<String,String>(mdcKeys.size());
            for (String s : mdcKeys) {
                Object mdc = event.getMDC(s);
                if (mdc != null) {
                	hashMap.put(s, mdc.toString());
                }
            }
        }
		return hashMap;
	}
	
	/***
	 * Sets the value of log4j.appender.logger_name.layout.MDCKeys from log4j.properties
	 * The value should be comma separated values
	 * 
	 * @param mDCKeys
	 */
	public void setMDCKeys(String mDCKeys){
        if (mDCKeys != null && mDCKeys.length() > 0){
            this.mdcKeys = Arrays.asList(mDCKeys.split(","));
        }
    }

	@Override
	public boolean ignoresThrowable() {
		return false;
	}

	@Override
	public void activateOptions() {
		//do nothing
	}


	public String getType() {
		return type;
	}
	
	/***
	 * Sets the value of log4j.appender.logger_name.layout.Type from log4j.properties
	 * 
	 * @param type
	 */
	public void setType(String type) {
		this.type = type;
	}
	
	
	
}
