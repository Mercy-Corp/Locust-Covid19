CREATE EXTERNAL TABLE `vegetation_fact`(
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
  's3://mercy-locust-covid19-reporting/vegetation_fact'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'transient_lastDdlTime'='1600168794', 
  'typeOfData'='file')
