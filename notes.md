# Things I can be doing:
* code to check whether the months actually add up to the quarters and sectors into districts
    * this may actually be unlikely because of how the sample looks. If it is a random sample of the real data, then it is much more likely that there's information for each cardholder sector spending in the smallest area they could give us, which would usually NOT be sector
    * or maybe it is bc there is sector-to-sector data for liverppol to edinburgh but only sector-to-area for glasgow to edinburgh (G34 0 to EH) in the sample, when one would expect many more spenders in edinburgh from glasgow than from liverpool
    * how: 
    1. [/] find a pair of postcodes that have sector information for both and then look for if they also have district information - this will confirm if the csvs contain aggregated data for at least one postcode sector to district. but not that all of them do, so:
    2. if indeed there are postcode sectors that also have aggregate district information, check which ones and if it's all of them - for each cardholder: for each district, count up how many sectors have information 
        (low priority)
    * this could be changing the location db to have just location_name and location_type (along with all the meta info) instead of splitting location into sector district area region. How does this play with aggregating in power BI?
* think about how to fix things if the opposite ends up being the case
* [/] turning stuff from pandas / csv to sql
* [/] converting to (batched) stream processing to accommodate huge data
    * multiprocessing could be added later if needed
* [/] maybe think of a better way to create csvs and sql statements than turning into dataframe
* [/] load census info
* think about fixing the structure of my data and my code
    * maybe somehow use generators instead of sending the execution flow back and forth between files the way I'm doing it now
    * see if dicts are really necessary everywhere - e.g. category may be better with int indexes
* the way I'm dealing with dimensions is such a mess, use classes for dimension tables
* try this with the real sample data that I've got to see where it crashes
    * do something about handling skipped rows
* [/] get distance data
* [/] put distance matrix into database
    * combine it with rest of etl
* rename ids in dims to match fact tables, eg time.time_id instead of time.id (this will make PowerBI behave nicer about linking)
* [/] tell stephen about flexi hours stuff and plan a meeting with the scotrail students
* [/] chase up Network Rail about cloud VMs
* add a readme and other administrative stuff. maybe comments and docs
* remember that spend does not necessarily equate travel desire. eg hiking or wild camping


# Notes on if the data contains a mix of aggregate and non-aggregate data:
* this would be very annoying
* option 1:
    * keep all rows as in raw data and have sector->sector and district->sector and area->sector multi-counted 
* option 2:
    * create an entry like rest_of_district for deaggregating district info (and others). consider how to locations
        * for each time period whether that is month quarter year
        * maybe have a different location_other associated with each time period to get more accurate times and shape files

* what if months to quarters also mix aggregated uhghghghg yikes that would be so disgusting
    * surely they wouldn't do that right??? how would they even decide where to non-agg by location and where to non-agg by month
        * very possible they just discard all entries which are <5 (instead of just putting in a 0 or a 5)
    * it's possible some quarters may have more detailed location information than their constituent months (e.g. Q1 may have sector->sector but Jan may have only district->sector)
    * maybe ignore quarterly data for now. Just filter them out until later.


There are ~150,000,000 postcode sector combinations (12,381^2) in the UK. If every one of them had an entry in the csv table, that would take up roughly 30GB for just ONE month. Expecting near 4GB per module, which just about rules out the possibility of there being all of the sector->sector information, so some is missing by aggregation. In fact most will likely be missing to aggregation.

Now to decide if keep like raw and (think of a map where the closer you are to a merchant the more granular the data is). Or create rest_of_district type things for each district. AND maybe for each month. BUT maybe not, because what if districts are either fully granularised or not granularised at all? (ie no partial aggregation which is good) WHY IS THE DATA SO LATEEE ugh.

May be best to have separate lookup tables in the db for all census info and travel durations etc but only fill the fact and dimension tables from there as they show up in the input data. OR have the census table full and ready with all the locations and then just get the ID of the relevant one and then have a ton of unused info in the dimension table but this should be okay right? maybe depends how much the db engine slows down as N grows.

# SQL add user:
    create user 'temp_user'@'localhost' identified by 'password';
    create databse sgov;
    grant create, drop, select, insert, execute, alter, index, update, delete on sgov.* to 'temp_user'@'localhost';

Power BI is being annoying, need to duplicate tables (New Table; newT = 'originalT')

To allow loading:
mysql> show global variables like 'local_infile';
mysql> set global local_infile=true;
bash>  mysql -u temp_user -p password --local_infile=true; 
py>    SQLALCHEMY_DATABASE_URI = "mysql://user:pass@localhost/dbname?charset=utf8&local_infile=1"

mysql> CREATE TABLE temp (header1 varchar(255) not null, header2 int not null, ... , IDKey int not null auto_increment, primary key(IDKey));
mysql> LOAD DATA LOCAL INFILE '/media/fpavlica/Data/school/work/2022_summer_scotgov/code/pre_work/temp_csv.csv' INTO TABLE temp FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (header1, header2, ...); 


# Notes on stream processsing and concurrency:
What needs to be processed in a batch stream straight into a database:
* fact table

What can be left to be processed in memory first:
* literally all of the other dimensions???
    * maybe not location? but even that should be no more than 25000 different ones which is nothing. Most likely only 12500
    * possibly not travel durations. Those may be too big to keep in memory. but maybe not. I don't know.
* but how do I deal with dicts and race conditions when doing more than 1 operation (eg check if not in and add)
    * lock = threading.Lock(); lock.acquire(); do_stuff(); lock.release()
    * (or: with lock.acquire(): do_stuff();)
    * make sure to avoid deadlocks by not locking two things at once and also adding a timeout
    * maybe instead use multiprocessing.Manager().lock, then "with lock:"
        * but really don't bc idk python is weird

# OTher notes:
Should I just do distance and census data load separately into db and then put census into location and distance into fact table?
How else would I do distances/durations if not fact table?: A lookup table from origin+destination to list of durations in different modes
A separate lookup table will be slower (to read) and also take up more space (storing just numbers)
    but will keep fact table cleaner. but is that really necessary??
But saving into fact table will need a LOT of memory if I want it to be any fast. 150MB per duration/distance stored. (use pandas at() faster than loc())
Means I might need to get the distances/durations ad-hoc for only the ones that are needed. This would be SO MUCH easier if I had the data ffs.

Will probably want to deal with distances separately - probably go through all the fact1 data to just get a list of journeys needed, then get the travel durations for those

    pd.set_option('display.max_columns', 20)


~~\~ The rest of this section is no longer relevant but leaving it here because these are just my notes \~~~

Turns out my row-wise solution is more than 2x faster than using SQL or Dask column-wise, and it makes it easier to do multiple fact tables (if I'm keeping dimension tables across different fact tables)

uhhhhhhh
I may have been doing things wrong this whole time
I've been ETL'ing row-wise instead of column-wise.
I've been going in loops for each row, do things like load into dimension table, look up dim table ID, then save into fact table.
When I could have loaded each whole column into its dimension, given it an ID, loaded the whole raw table into a db, matched (joined) on appropriate things with the now-created dimension tables and just replace the raw strings with their IDs (or even done a map-type replace if faster)
Could use spark for that
Probably even pandas would've been fine, or even just SQL??
Talk about this with Andy on Monday.
It makes so much sense
Why didn't I think of this before


Let's rethink.
Likely going to use Apache Spark but let's think if I can use just SQL, maybe through some ORM like SQLAlchemy.
(Might be better to just do it in Spark and then if I find it's too slow and transferable to SQL I can do that.)
Might also be worth considering doing a load raw csv into sql and then the operations in SQLAlchemy or Spark.

The steps I'd need (for file 1):
0. drop everything
1. Load CSV raw-ish
2. select distinct times, turn into year/quarter/month and save into table
4. select union of distict cardholder loc and merchant loc, de-granularise and save into table
5. select distinct from category (1 2 and 3), union of those and into table
6. inner join raw with time, location on cardholder, location on merchant, category on cat1 cat2 cat3, (probably one by one)
7. select pan_cnt, txn_cnt, txn_gbp_amt, time_id, ch_id, merc_id, cat1_id, cat2_id, cat3_id from that
8. and that's my ETL'd table!
9. to load following fact tables, always do union with time and location tables instead of replacing

For getting distances:
Probably load all distances into their own table with indexes on origin and destination, and then do a join: fact table join distances on ft.ch_loc = dist.origin and ft.merc_loc = dist.destination (either with original strings on both or IDs on both. IDs may be faster but will need creating somehow)

# notes on speed:
MySQL built-in load csv takes 10s on the raw dataset or 5.2s on the csv (with 1 mil rows), when no id provided (6s with ID). So it won't get any faster than that (same performance with both InnoDB and MyISAM.)
So I guess my current limit is ~13s with the intermediate csv step - 7s for process, 6s for load.
And my current speed with batch size 10,000 and unproteced querys is 16s. same w batch size 1000. 19 seconds for size 100, 88 seconds for size 1. Seems faster with powers of 10^k than 2*10^k (eg 1000 faster than 2000).
25s with a parameterised query batch 1000.
20s with escaping and encoding strings, bacth 1000.
17s with a type int float assert

See https://www.slideshare.net/billkarwin/load-data-fast (esp slide 50) for a speed comparison 

Time inserting (with transformations) of 1 million rows:
* SQL (experimental/file1_sql.py):     42 seconds
* Dask (experimental/file1_dask.py):   55 seconds (38 of that is uploading fact table to_sql)
* my row-wise solution (etl/file1.py): 18 seconds

* 4 batches of 1k ended up slower than 1 batch of 4k for each thread


Time to convert two rows into dict-like Series (index is key, value is value):
> \>>> timeit.timeit("df.set_index('ids')['vals']", "import pandas as pd; df = pd.DataFrame({'ids':range(10), 'vals':list('ABCDEFGHIJ')})", number=1000)  
> 0.22599304300092626

> \>>> timeit.timeit("pd.Series(df['vals'].values, df['ids'])", "import pandas as pd; df = pd.DataFrame({'ids':range(10), 'vals':list('ABCDEFGHIJ')})", number=1000)  
> 0.06841536700085271

> \>>> timeit.timeit("pd.Series(ddf['vals'].compute().values, ddf['ids'].compute())", "import pandas as pd; import dask.dataframe as dd; ddf = dd.from_pandas(pd.DataFrame({'ids':range(10), 'vals':list('ABCDEFGHIJ')}), chunksize=20)", number=1000)
> 1.3082972429983784

>>> timeit.timeit("dd.from_pandas(pd.Series(ddf['vals'].compute().values, ddf['ids'].compute()), chunksize=20)", "import pandas as pd; import dask.dataframe as dd; ddf = dd.from_pandas(pd.DataFrame({'ids':range(10), 'vals':list('ABCDEFGHIJ')}), chunksize=20)", number=1000)
1.4999407619980047

>>> timeit.timeit("df = ddf[['vals', 'ids']].compute();pd.Series(df['vals'].values, df['ids'])", "import pandas as pd; import dask.dataframe as dd; ddf = dd.from_pandas(pd.DataFrame({'ids':range(10), 'vals':list('ABCDEFGHIJ')}), chunksize=20)", number=1000)
1.8768360009999014

>>> timeit.timeit("ddf.set_index('ids')['vals']", "import pandas as pd; import dask.dataframe as dd; ddf = dd.from_pandas(pd.DataFrame({'ids':range(10), 'vals':list('ABCDEFGHIJ')}), chunksize=20)", number=1000)
6.46500810499856






# GraphHopper shenanigans:
* for how to use in code, see /example/src/main/java/com/graphhopper/example/RoutingExample.java
* for public transport version, should be just a plug'n'play sort of deal with replacing the child class GraphHopperPt for GraphHopper
* for public transport times, probably get list of instructions and add up the times rather than trying to figure out something from GraphHopperPt
    * if wanting no java writing, consider line 209 of /reader-gtfs/.../gtfs/PtRouterImpl.java:
        * responsePath.setTime((solution.get(solution.size() - 1).label.currentTime - solution.get(0).label.currentTime));
        * maybe replace first currentTime with departureTime
    * but would highly encourage writing this in java because the API is very slow even on localhost, and the responses file is too big to transfer easily, and it is too big for my laptop to load into memory :/
    * so it looks like I might have to fork GraphHopper and write my own code for getting public transit times
    * or give up!
* the main file is /core/src/main/java/com/graphhopper/GraphHopper.java
* traveline data takes forever to build - may be worth buying a crazy powerful server just to build the graphs and then serve it from a weaker one.
* web interfaces seem to be in /web-bundle/.../graphhopper/resources/*Resource.java
    * but please don't use the web interface, it takes forever.
    * fun fact, the ORS matrix service took about 45 seconds on localhost to get a matrix of roughly ~1,200,000, GH public transit individual routes are taking 45 MINUTES to get 19,200 routes - some of it might be in the harder public transit routing, but I'd guess most of it will be in the interface (sending and receiving  19200 individual requests rather than a single long one)


# What to do with partially aggregated rows:
* on encountering a postcode_district row:
    * save it for later (hopefully memory should be enough?)
* once all done reading in:
    * go through saved (district) rows and for each one:
        * find all fact table rows where sector is in district. origin also matches (may also need to match date ranges)
            * (this means find IDs of locs where sector[:-2] = district_destination, and where sector=origin. And then select from fact1 where loc is one of those IDs)
        * sum up measures values of the found rows with the sector matching
        * subtract the summed found sectors from district row values
        * save this as row_other (maybe also _time, or _ list of missing value sectors eg _EH1-6_EH1-7_EH1-8 if EH1 1 through 5 were present)
        * still to deal with: distances and population
        * average out distances somehow. If they are in the fact table, probably just get avg(distance) of missing. (The relevant values will not be in the fact table). If they are separate, get distances for all matched IDs/sectors and average out those. This will require distance table to be fully full with every NxN journey, huge matrix. Maybe just get distances for districts somehow instead of every single sector
            * maybe only do distances after (separately)
        * population is summed across the matched missing sectors

Really rethink what problem I'm trying to solve here. Do I really need to do all this work. And is there more sector info than district info etc.

How about separate sector district area dimensions instead of a location one 

_other is not an option. why? because each dest district has many origin sectors, and those origin sectors may each have a different selection of dest sectors (eg. DD1 1 may have data for EH3 1 and EH3 2 but not the rest, and KY1 1 may have data for EH3 7 and EH3 8 but not EH3 1 or EH3 2. therefore EH3_other would be different for every origin sector.)
so instead probably best estimating sector values, weighed by sector population.
But that produces ENORMOUS data. (150 mil sector-to-sector rows, which would take ~30GB space, and that's just one month)
So I don't think getting around the aggreg limitations from this angle is the way to do it.
Maybe just have values for overall and areas and districts and sectors separately, but smaller data that way, also inconsistency across destination sectors so they cannot be compared

How about just replace the value where district row would be with district minus sum(subsectors)?
Then I don't have a district_other value and only a district value, which should be okay???
    What does that break? 
    Measure values - should stay consistent and become aggregable
    Shape file stay full size and are just drawn under the sectors - hopefully okay.
    Distances will need extra row of avg distance per district sectors
    population - should be fine because added up over sectors. but might have to put population as 0 in the location table row

ie select sum where origin sector is same and destination district is curr_district, subtract that from measure, for all measures, and save into db as new row

This might actually be okay to save for after the data arrives in case they already have done that



# Things to discuss with Andy:
* form of final report - passive or active form (I did thing or thing was done)