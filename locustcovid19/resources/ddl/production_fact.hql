CREATE EXTERNAL TABLE `production_fact`(
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
  's3://mercy-locust-covid19-reporting/production_fact'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='production_fact', 
  'averageRecordSize'='17', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='116', 
  'sizeKey'='5336', 
  'transient_lastDdlTime'='1600777698', 
  'typeOfData'='file')
