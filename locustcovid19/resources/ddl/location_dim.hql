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
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/location_dim'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='location_dim', 
  'averageRecordSize'='17', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='116', 
  'sizeKey'='5336', 
  'transient_lastDdlTime'='1600844601', 
  'typeOfData'='file')
