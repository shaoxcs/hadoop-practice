DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

flight1 = LOAD 's3://cs6240-shao/data.csv'
          USING CSVLoader(',');

flight2 = LOAD 's3://cs6240-shao/data.csv'
          USING CSVLoader(',');

f1 = FOREACH (
        FILTER flight1
        BY $11 EQ 'ORD'
        AND (int)$41 != 1
        AND (int)$43 != 1
        AND (chararray)$5 >= '2007-06-01'
        AND (chararray)$5 < '2008-06-01')
     GENERATE (chararray)$5 AS FlightDate:chararray,
              (chararray)$11 AS Origin:chararray,
              (chararray)$17 AS Dest:chararray,
              (chararray)$24 AS DepTime:chararray,
              (chararray)$35 AS ArrTime:chararray,
              (double)$37 AS ArrDelayMinutes:double;

f2 = FOREACH (
        FILTER flight2
        BY $17 EQ 'JFK'
        AND (int)$41 != 1
        AND (int)$43 != 1
        AND (chararray)$5 >= '2007-06-01'
        AND (chararray)$5 < '2008-06-01')
     GENERATE (chararray)$5 AS FlightDate:chararray,
              (chararray)$11 AS Origin:chararray,
              (chararray)$17 AS Dest:chararray,
              (chararray)$24 AS DepTime:chararray,
              (chararray)$35 AS ArrTime:chararray,
              (double)$37 AS ArrDelayMinutes:double;

twolegs = JOIN f1 BY (FlightDate, Dest), f2 BY (FlightDate, Origin);

possibleTwoLegs = FILTER twolegs BY f1::ArrTime < f2::DepTime;

delays = FOREACH possibleTwoLegs GENERATE $5+$11 AS delay:double;

d = GROUP delays ALL;

result = FOREACH d GENERATE COUNT_STAR(delays) AS count:int, AVG(d.delays) AS average:double;

STORE result INTO 's3://cs6240-shao/pigout-filterfirst';