CREATE EXTERNAL TABLE `demand_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` bigint, 
  `locationid` string, 
  `value` double, 
  `dm_commodity_name` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mercy-locust-covid19-out-dev/demand_fact/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='demand_fact', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='54', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'='|', 
  'objectCount'='1', 
  'recordCount'='9434', 
  'sizeKey'='509456', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')
