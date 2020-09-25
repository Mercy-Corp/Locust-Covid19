CREATE EXTERNAL TABLE `date_dim`(
  `dateid` bigint, 
  `date` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/Date_Dim'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='date_dim', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='15', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='1', 
  'recordCount'='15065', 
  'sizeKey'='225980', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1600844239', 
  'typeOfData'='file')
