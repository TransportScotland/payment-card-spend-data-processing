# Things I can be doing:
* code to check whether the months actually add up to the quarters and sectors into districts
    * this may actually be unlikely because of how the sample looks. If it is a random sample of the real data, then it is much more likely that there's information for each cardholder sector spending in the smallest area they could give us, which would usually NOT be sector
    * or maybe it is bc there is sector-to-sector data for liverppol to edinburgh but only sector-to-area for glasgow to edinburgh (G34 0 to EH) in the sample, when one would expect many more spenders in edinburgh from glasgow than from liverpool
    * how: 
    1. [/] find a pair of postcodes that have sector information for both and then look for if they also have district information - this will confirm if the csvs contain aggregated data for at least one postcode sector to district. but not that all of them do, so:
    2. if indeed there are postcode sectors that also have aggregate district information, check which ones and if it's all of them - for each cardholder: for each district, count up how many sectors have information 
        (low priority)
* think about how to fix things if the opposite ends up being the case
* [/] turning stuff from pandas / csv to sql
* [/] converting to (batched) stream processing to accommodate huge data
    * multiprocessing could be added later if needed
* [/] maybe think of a better way to create csvs and sql statements than turning into dataframe
* [/] load census info
* think about fixing the structure of my data and my code
    * maybe somehow use generators instead of sending the execution flow back and forth between files the way I'm doing it now
    * see if dicts are really necessary everywhere - e.g. category may be better with int indexes
* try this with the real sample data that I've got to see where it crashes
* rename ids in dims to match fact tables, eg time.time_id instead of time.id


# SQL add user:
    create user 'temp_user'@'localhost' identified by 'password';
    create databse sgov;
    grant create, drop, select, insert, execute, alter, index on sgov.* to 'temp_user'@'localhost';

Power BI is being annoying, need to duplicate tables (New Table, newT = 'originalT')

# Notes on stream processsing and concurrency:
What needs to be processed in a batch stream straight into a database:
* fact table

What can be left to be processed in memory first:
* literally all of the other dimensions???
    * maybe not location? but even that should be no more than 25000 different ones which is nothing. Most likely only 12500
* but how do I deal with dicts and race conditions when doing more than 1 operation (eg check if not in and add)
    * lock = threading.Lock();; lock.acquire(); do_stuff(); lock.release()
    * (or: with lock.acquire(): do_stuff();)
    * make sure to avoid deadlocks by not locking two things at once and also adding a timeout
    * maybe instead use multiprocessing.Manager().lock, then "with lock:"
        * but really don't bc idk python is weird

# notes on speed:
MySQL built-in load csv takes 10s on the raw dataset or 5.2s on the csv (with 1 mil rows), when no id provided (6s with ID). So it won't get any faster than that (same performance with both InnoDB and MyISAM.)
So I guess my current limit is ~13s with the intermediate csv step - 7s for process, 6s for load.
And my current speed with batch size 10,000 and unproteced querys is 16s. same w batch size 1000. 19 seconds for size 100, 88 seconds for size 1. Seems faster with powers of 10^k than 2*10^k (eg 1000 faster than 2000).
25s with a parameterised query batch 1000.
20s with escaping and encoding strings, bacth 1000.
17s with a type int float assert 

See https://www.slideshare.net/billkarwin/load-data-fast (esp slide 50) for a speed comparison 