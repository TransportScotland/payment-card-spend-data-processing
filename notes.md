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
* the way I'm dealing with dimensions is such a mess, use classes for dimension tables
* try this with the real sample data that I've got to see where it crashes
    * do something about handling skipped rows
* [/] get distance data
* [/] put distance matrix into database
* rename ids in dims to match fact tables, eg time.time_id instead of time.id (this will make PowerBI behave nicer about linking)
* [/] tell stephen about flexi hours stuff and plan a meeting with the scotrail students
* [/] chase up Network Rail about cloud VMs
* add a readme and other administrative stuff. maybe comments and docs


# SQL add user:
    create user 'temp_user'@'localhost' identified by 'password';
    create databse sgov;
    grant create, drop, select, insert, execute, alter, index, update, delete on sgov.* to 'temp_user'@'localhost';

Power BI is being annoying, need to duplicate tables (New Table; newT = 'originalT')

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
