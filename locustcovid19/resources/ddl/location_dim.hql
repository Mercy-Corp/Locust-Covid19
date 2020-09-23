CREATE EXTERNAL TABLE `location_dim`(
  `locationid` string, 
  `name` string, 
  `type` string, 
  `hierarchy` bigint, 
  `gid_0` string, 
  `gid_1` string, 
  `gid_2` string, 
  `gid_3` string, 
  `name_0` string, 
  `name_1` string, 
  `name_2` string, 
  `name_3` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '|' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mercy-locust-covid19-out-dev/location_dim/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='location_dim', 
  'areColumnsQuoted'='false', 
  'averageRecordSize'='52', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'='|', 
  'objectCount'='1', 
  'recordCount'='8184', 
  'sizeKey'='425603', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')
