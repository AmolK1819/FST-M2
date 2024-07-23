--- Load Data from HDFS
inputDialogues4 = LOAD 'hdfs:///user/amol/inputs/episodeIV_dialogues.txt' USING PigStorage('\t) AS (name:chararray, line:chararray);
inputDialogues5 = LOAD 'hdfs:///user/amol/inputs/episodeV_dialogues.txt' USING PigStorage('\t) AS (name:chararray, line:chararray);
inputDialogues6 = LOAD 'hdfs:///user/amol/inputs/episodeVI_dialogues.txt' USING PigStorage('\t) AS (name:chararray, line:chararray);

--- Filter out first two lines from each file
ranked4 = RANK inputDialogues4;
OnlyDialogues4 = FILTER ranked4 By (rank_inputDialogues4 > 2);
ranked5 = RANK inputDialogues4;
OnlyDialogues4 = FILTER ranked5 By (rank_inputDialogues5 > 2);
ranked6 = RANK inputDialogues4;
OnlyDialogues4 = FILTER ranked6 By (rank_inputDialogues6 > 2);

--- Merge the three inputs
inputData = UNION OnlyDialogues4, OnlyDialogues5, OnlyDialogues6;

--- Group by name
groupByName = GROUP inputData BY name;

--- Count the number of lines by each character
names = FOREACH groupByNames GENERATE $0 as names, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

--- Remove the outputs folder
rmf hdfs:///user/amol/outputs;

--- Store result in HDFS
STORE namesOrdered INTO 'hdfs:///user/amol/outputs' USING PigStorage('\t);
