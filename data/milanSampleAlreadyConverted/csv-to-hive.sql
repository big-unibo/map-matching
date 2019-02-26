CREATE TABLE osmwaysmilan (id string,userid bigint,timestamp string,isvisible boolean ,version int,changesetid bigint,tags map<string,string>,nodes array<string>) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '${hiveconf:pathways}' OVERWRITE INTO TABLE osmwaysmilan;
CREATE TABLE osmnodesmilan (id string,userid bigint,timestamp string,isvisible boolean,version int,changesetid bigint,tags map<string,string>,latitude double,longitude double) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH '${hiveconf:pathnodes}' OVERWRITE INTO TABLE osmnodesmilan;