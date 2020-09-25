CREATE EXTERNAL TABLE `forageland_locust_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` bigint, 
  `locationid` string, 
  `value` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/forageland_locust_fact'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='forageland_locust_fact', 
  'averageRecordSize'='18', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='817', 
  'sizeKey'='18399', 
  'transient_lastDdlTime'='1600777384', 
  'typeOfData'='file')
