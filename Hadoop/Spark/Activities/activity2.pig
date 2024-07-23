-- Load input file from HDFS
inputFile = LOAD 'hdfs:///user/Amol/input.txt' AS (line:chararray);
-- Tokenize the lines for each word in the file (Map)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
totalCount = FOREACH grpd GENERATE $0, COUNT($1);
-- Remove the old results
rmf hdfs:///user/Amol/pigOutput1;
-- Store the result in HDFS
STORE totalCount INTO 'hdfs:///user/Amol/pigOutput1';
