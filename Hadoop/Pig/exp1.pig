-- load data
census = LOAD 'gaz_tracts_national.txt' as (USPS: chararray, GEOID: int, POP10: int, HU10: int, ALAND: long, AWATER: long, ALAND_SQMI: double, AWATER_SQMI: double, INTPTLAT: double, INTPTLONG: double);

-- only aggregate the fields being used
state_land = FOREACH census GENERATE USPS, ALAND;

-- group the land by state location
state_group = GROUP state_land BY USPS;

-- sum the amount of land in each state
state_sums = FOREACH state_group GENERATE state_land.USPS AS states, SUM(state_land.ALAND) AS land_sum: long;

-- flatten the data
flatten_state = FOREACH state_sums GENERATE FLATTEN(states.USPS) as state, land_sum;
-- remove redundant information about total land per state
unique_states = DISTINCT flatten_state;

-- sort and grab the top 10
sorted = ORDER unique_states BY land_sum DESC PARALLEL 4;
top_10 = LIMIT sorted 10;
STORE top_10 INTO 'lab6';