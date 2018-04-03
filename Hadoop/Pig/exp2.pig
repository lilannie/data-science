set default_parallel 10;

-- load the data
log = LOAD '/cpre419/network_trace' USING PigStorage(' ') AS (time: chararray, ip: chararray, source: chararray, s: chararray, destination: chararray, protocol: chararray, data: chararray);

-- remove non significant connections (i.e. ones that aren't tcp
only_tcp = FILTER log BY protocol == 'tcp';

-- reformat the ip addresses and remove unused fields
clean_log = FOREACH only_tcp GENERATE REPLACE(source, '(.([0-9]+|[0-9]+:))$', '') AS source, REPLACE(destination, '(.([0-9]+|[0-9]+:))$', '') AS destination;

-- group the logs by source ip
source_ips = GROUP clean_log BY source;

-- count the number of distinct destination ip addresses
count_dest = FOREACH source_ips {
    unique_dest = DISTINCT clean_log.destination;
    GENERATE group as source, COUNT(unique_dest) as destination_count;
}

-- sort and get the top 10 datasets
sorted = ORDER count_dest BY destination_count DESC;
top_10 = LIMIT sorted 10;
STORE top_10 INTO '/user/lilannie/lab6/exp2/output/';

