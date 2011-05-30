package com.avrolog.log4j.layout.test;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

public class Test {
	
	 static Logger logger = Logger.getLogger(Test.class	); 

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		testDebug();
		testInfo();
		testWarn();
		testMDC();		
		
	}
	
	private static void testDebug(){
		logger.debug("This is a debug message");
	}
	
	private static void testInfo(){
		logger.info("This is a info message");
	}
	

	private static void testWarn(){
		logger.info("This is a warn message");
	}
	
	private static void testMDC(){
		MDC.put("mdcKey", "mdcVal");
		logger.info("This is a warn message");
	}


}
