package com.avrolog.log4j.layout;

import java.io.File;
import java.io.FileInputStream;

import org.apache.avro.Schema;

/***
 * Util for loading schema
 * 
 * @author harisgx
 *
 */
public class SchemaUtil {	

	private static final String schemafileName = "avro.schema";
	private volatile Schema schema;
	private static volatile SchemaUtil schemaUtil;
	
	/***
	 * 
	 * @return instance
	 */
	public static SchemaUtil instance(){
		if(schemaUtil == null){
			synchronized (SchemaUtil.class) {
				schemaUtil = new SchemaUtil();
			}			
		}
		
		return schemaUtil;
	}
	
	/***
	 * 	
	 * @return schema instance
	 */
	public Schema getSchema(){		
		try{		
			if(schema == null){
				synchronized (this) {
					String schemaStr = loadSchema();
					schema = Schema.parse(schemaStr);
				}			
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return schema;
	}
	
	private String loadSchema() throws Exception{
		
		FileInputStream in = null;		
		File schemaFile = new File(getClass().getResource(schemafileName).getPath());
		String schemaStr = null;
		
		try{
			in = new FileInputStream(schemaFile);
			StringBuilder out = new StringBuilder();
			byte[] b = new byte[4096];
			for (int n; (n = in.read(b)) != -1;) {
			     out.append(new String(b, 0, n));
			}
			
			schemaStr = out.toString();
			
		}catch(Exception e){
			throw e;
		}finally{
			if(in != null){
				in.close();
			}
		}
	
		return schemaStr;
	}

}
