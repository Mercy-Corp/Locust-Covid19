CREATE EXTERNAL TABLE `displacement_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` int, 
  `locationid` string, 
  `value` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/displacement_fact'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='displacement_fact', 
  'averageRecordSize'='13', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='120', 
  'sizeKey'='4833', 
  'transient_lastDdlTime'='1600789984', 
  'typeOfData'='file')
