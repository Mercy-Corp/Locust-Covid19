CREATE EXTERNAL TABLE `price_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` bigint, 
  `locationid` string, 
  `value` double, 
  `commodity_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mercy-locust-covid19-out-dev/price_fact/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='price_fact', 
  'averageRecordSize'='10', 
  'classification'='parquet', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='2', 
  'recordCount'='24371', 
  'sizeKey'='252430', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')
