CREATE EXTERNAL TABLE `violence_fact`(
  `factid` string, 
  `measureid` bigint, 
  `dateid` int, 
  `locationid` string, 
  `value` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://mercy-locust-covid19-reporting/violence_fact'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'transient_lastDdlTime'='1599125836')
