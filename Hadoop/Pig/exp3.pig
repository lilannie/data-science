set default_parallel 10;

-- PART A --
-- load the data
ip_trace = LOAD '/cpre419/ip_trace' USING PigStorage(' ') AS (time: chararray, connection_id: chararray, source: chararray, s: chararray, destination: chararray, protocol: chararray, data: chararray);
raw_block = LOAD '/cpre419/raw_block' USING PigStorage(' ') AS (connection_id: chararray, action: chararray);

-- remove any unused fields
ip_clean = FOREACH ip_trace GENERATE time, connection_id, source, destination;
-- only use the Blocked rows
raw_block_clean = FILTER raw_block BY action == 'Blocked';

-- Join the tables
join_result = JOIN ip_clean BY connection_id, raw_block_clean BY connection_id;

-- Remove redundant information
firewall = FOREACH join_result GENERATE ip_clean::time as time, ip_clean::connection_id as connection_id, ip_clean::source as source, ip_clean::destination as destination, raw_block_clean::action as action;

-- Print out the firewall file
STORE firewall INTO '/user/lilannie/lab6/exp3/firewall';

-- PART B --
-- remove unused fields
source_blocked = FOREACH firewall GENERATE connection_id, source;

-- group the blocked records by source ip
source_grouped = GROUP source_blocked by source;

-- count the number of distinct blocked times
sources = FOREACH source_grouped {
    unique_times = DISTINCT source_blocked.connection_id;
    GENERATE group as source, COUNT(unique_times) as times_blocked;
}

-- sort the data
sorted = ORDER sources BY times_blocked DESC;
STORE sorted INTO '/user/lilannie/lab6/exp3/output';

