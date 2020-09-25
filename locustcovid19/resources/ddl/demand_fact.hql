CREATE EXTERNAL TABLE `demand_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` bigint, 
  `locationid` string, 
  `value` double, 
  `dm_commodity_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/demand_fact'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='cropland_fact', 
  'averageRecordSize'='20', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='844', 
  'sizeKey'='20362', 
  'transient_lastDdlTime'='1600784508', 
  'typeOfData'='file')
