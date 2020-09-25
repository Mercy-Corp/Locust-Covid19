CREATE EXTERNAL TABLE `measure_dim`(
  `measureid` bigint, 
  `measure` string, 
  `date granularity` string, 
  `location granularity` string, 
  `unit` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\;' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/measure_dim'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='measure_dim', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='61', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'='\;', 
  'objectCount'='1', 
  'recordCount'='25', 
  'sizeKey'='1554', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1600844959', 
  'typeOfData'='file')
