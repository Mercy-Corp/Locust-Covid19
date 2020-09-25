CREATE EXTERNAL TABLE `rvf_risk_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` int, 
  `locationid` string, 
  `value` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/rvf_risk_fact'
TBLPROPERTIES (
  'averageRecordSize'='10', 
  'classification'='parquet', 
  'compressionType'='none', 
  'transient_lastDdlTime'='1601028365', 
  'typeOfData'='file')
