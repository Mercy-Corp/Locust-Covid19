CREATE EXTERNAL TABLE `peaceindex_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` int, 
  `locationid` string, 
  `value` double)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\;' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/peaceindex_fact'
TBLPROPERTIES (
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
  'transient_lastDdlTime'='1597990959', 
  'typeOfData'='file')
