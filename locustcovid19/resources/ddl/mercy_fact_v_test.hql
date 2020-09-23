CREATE OR REPLACE VIEW mercy_fact_v_test AS 
SELECT
  "pf"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, "pf"."commodity_name" "name"
, "pf"."value"
, 'WFP/Numbeo/REACH' "Source_Name"
, '1' "set"
FROM
  (((price_fact_test pf
LEFT JOIN measure_dim_test m ON ("pf"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("pf"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("pf"."dateid" = "d"."dateid"))
WHERE ("pf"."factid" LIKE 'PR%')
UNION ALL SELECT
  "ff"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "ff"."value"
, 'GeoNetwork' "Source_Name"
, null "set"
FROM
  (((forageland_fact_test ff
LEFT JOIN measure_dim_test m ON ("ff"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("ff"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("ff"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "flf"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "flf"."value"
, 'GeoNetwork/LocustHub' "Source_Name"
, null "set"
FROM
  (((forageland_locust_fact_test flf
LEFT JOIN measure_dim_test m ON ("flf"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("flf"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("flf"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "clf"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "clf"."value"
, 'FAO/USGS/LocustHub' "Source_Name"
, null "set"
FROM
  (((cropland_locust_fact_test clf
LEFT JOIN measure_dim_test m ON ("clf"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("clf"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("clf"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "cf"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "cf"."value"
, 'USGS' "Source_Name"
, null "set"
FROM
  (((cropland_fact_test cf
LEFT JOIN measure_dim_test m ON ("cf"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("cf"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("cf"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "df"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, "dm_commodity_name" "name"
, "df"."value"
, 'FAO' "Source_Name"
, '1' "set"
FROM
  (((demand_fact_test df
LEFT JOIN measure_dim_test m ON ("df"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("df"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("df"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "dif"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, "dif"."disease_name" "name"
, "dif"."value"
, 'WAHIS' "Source_Name"
, null "set"
FROM
  (((disease_fact_test dif
LEFT JOIN measure_dim_test m ON ("dif"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("dif"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("dif"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "pof"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "pof"."value"
, 'WorldPop/UN' "Source_Name"
, null "set"
FROM
  (((population_fact_test pof
LEFT JOIN measure_dim_test m ON ("pof"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("pof"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("pof"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "prd"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "prd"."value"
, 'FAO' "Source_Name"
, '1' "set"
FROM
  (((production_fact_test prd
LEFT JOIN measure_dim_test m ON ("prd"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("prd"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("prd"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "prd"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "prd"."value"
, 'Vision of Humanity' "Source_Name"
, null "set"
FROM
  (((peaceindex_fact_test prd
LEFT JOIN measure_dim_test m ON ("prd"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("prd"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("prd"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "ter"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "ter"."value"
, 'Vision of Humanity' "Source_Name"
, null "set"
FROM
  (((terrorismindex_fact_test ter
LEFT JOIN measure_dim_test m ON ("ter"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("ter"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("ter"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "dis"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "dis"."value"
, 'World Bank' "Source_Name"
, null "set"
FROM
  (((displacement_fact_test dis
LEFT JOIN measure_dim_test m ON ("dis"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("dis"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("dis"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "con"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "con"."value"
, 'UCDP' "Source_Name"
, null "set"
FROM
  (((conflict_fact con
LEFT JOIN measure_dim_test m ON ("con"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("con"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("con"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "fact"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "fact"."value"
, 'UNHCR' "Source_Name"
, null "set"
FROM
  (((refugees_fact_test fact
LEFT JOIN measure_dim_test m ON ("fact"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("fact"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("fact"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "fact"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "fact"."value"
, 'ACLED' "Source_Name"
, null "set"
FROM
  (((violence_fact_test fact
LEFT JOIN measure_dim_test m ON ("fact"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("fact"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("fact"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "fact"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "fact"."value"
, 'ACLED' "Source_Name"
, null "set"
FROM
  (((cpi_fact_test fact
LEFT JOIN measure_dim_test m ON ("fact"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("fact"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("fact"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "famine"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "famine"."value"
, 'ACLED' "Source_Name"
, null "set"
FROM
  (((famine_fact_test famine
LEFT JOIN measure_dim_test m ON ("famine"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("famine"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("famine"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "fact"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "fact"."value"
, 'ICPAC' "Source_Name"
, null "set"
FROM
  (((locust_risk_fact_test fact
LEFT JOIN measure_dim_test m ON ("fact"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("fact"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("fact"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "fact"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "fact"."value"
, 'ICPALD' "Source_Name"
, null "set"
FROM
  (((rvf_risk_fact_test fact
LEFT JOIN measure_dim_test m ON ("fact"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("fact"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("fact"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "prd"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "prd"."value"
, 'FAO' "Source_Name"
, '2' "set"
FROM
  (((production_fact_test prd
LEFT JOIN measure_dim_test m ON ("prd"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("prd"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("prd"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "df"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, "dm_commodity_name" "name"
, "df"."value"
, 'FAO' "Source_Name"
, '2' "set"
FROM
  (((demand_fact_test df
LEFT JOIN measure_dim_test m ON ("df"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("df"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("df"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "pf"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, "pf"."commodity_name" "name"
, "pf"."value"
, 'WFP/Numbeo/REACH' "Source_Name"
, '2' "set"
FROM
  (((price_fact_test pf
LEFT JOIN measure_dim_test m ON ("pf"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("pf"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("pf"."dateid" = "d"."dateid"))
WHERE ("pf"."factid" LIKE 'PR%')
UNION ALL SELECT
  "fact"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "fact"."value"
, 'USGS' "Source_Name"
, null "set"
FROM
  (((vegetation_fact_test fact
LEFT JOIN measure_dim_test m ON ("fact"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("fact"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("fact"."dateid" = "d"."dateid"))
UNION ALL SELECT
  "fact"."factid"
, "m"."measure"
, "l"."locationid"
, "l"."hierarchy"
, "l"."name_0" "location_1"
, "l"."name_1" "location_2"
, "l"."name_2" "location_3"
, "l"."name_3" "location_4"
, "date_parse"("d"."date", '%m/%d/%Y') "date"
, null "name"
, "fact"."value"
, 'WorldBank' "Source_Name"
, null "set"
FROM
  (((financial_inclusion_fact_test fact
LEFT JOIN measure_dim_test m ON ("fact"."measureid" = "m"."measureid"))
LEFT JOIN location_dim_test l ON ("fact"."locationid" = "l"."locationid"))
LEFT JOIN date_dim_test d ON ("fact"."dateid" = "d"."dateid"))
