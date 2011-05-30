### How to use

  	log4j.appender.logger_name.layout=com.avrolog.log4j.layout.AvroLogLayout
  	log4j.appender.logger_name.layout.Type=json
  	log4j.appender.logger_name.layout.MDCKeys=mdcKey
  
  
  Provide the MDC keys as comma seperated values 
  
  add the required libraries to class path
  
### TODO

	- supporting binary serialization
	- custom appenders
	
  
  
  *[more details] (http://bytescrolls.blogspot.com/2011/05/using-avro-to-serialize-logs-in-log4j.html)