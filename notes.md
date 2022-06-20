# Things I can be doing:
* [/] code to check whether the months actually add up to the quarters and sectors into districts
    * this may actually be unlikely because of how the sample looks. If it is a random sample of the real data, then it is much more likely that there's information for each cardholder sector spending in the smallest area they could give us, which would usually NOT be sector
    * or maybe it is bc there is sector-to-sector data for liverppol to edinburgh but only sector-to-area for glasgow to edinburgh (G34 0 to EH) in the sample, when one would expect many more spenders in edinburgh from glasgow than from liverpool
    * how: 
    1. find a pair of postcodes that have sector information for both and then look for if they also have district information - this will confirm if the csvs contain aggregated data for at least one postcode sector to district. but not that all of them do, so:
    2. if indeed there are postcode sectors that also have aggragate district information, check which ones and if it's all of them - for each cardholder: for each district, count up how many sectors have information 
        (low priority)
* think about how to fix things if the opposite ends up being the case
* turning stuff from pandas to sql
* converting to (batched) stream processing to accommodate huge data
* maybe think of a better way to create csvs and sql statements than turning into dataframe
* load census info