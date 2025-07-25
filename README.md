

get some data from fakerapi
convert into a format that can be ingested by bigquery
write out the data to google cloud storage

get the json of multiple users
has a total
then array of json

we only need 100. 


### what format 

csv - easy, readable, can check quick. no schema enforcement, has to be standardised. no compression. 
json - again easy readable can check quick. Schema enforcement through key,value pairs. good for sending on downstream.
avro - for hadoop, serialized, includes schema. but slower and not easy to check. row-oriented. 
parquet - stores data by column, basically perfect for bigquery, good for analytics queries, strong compression. but bugger to work with & check. 
orc - same as parqut but more apache hive focussed/slightly less widely used imo. 

how granular do we store the data? 


for simplicity, json is best. 
in very large datasets, avro would offer better performance.
parquet is best for within bigquery, but then conversion would be extra step. 

anything that self-defines a schema would make it easier for bigquery to ingest
depends where we want the work to be? 
can we afford to let things pass through the cracks? 
or do we have a data layer? let this loading layer just do one thing - more aligned with microservice principals. 

in the ingestion process, we're using Python (not strongly typed like go) - but can still enforce data types at source by choice of data format. 
However, it does tie us to a schema. This is both a good and bad thing, much less flexible, but fewer drift surprises. 

do we turn this into more of an ETL pipeline? generally speaking I'd say no, have another separate thing responsible for the transformation. This is the extract step. But as this is a deng task maybe we should. 
This is user data, it's sensitive and important. With IP addresses. 

it might also have missing fields. 

### structure and decisions to make  

could just do a classic producer consumer paradigm, with buffers to keep memory usage low. concurrency and decoupling

- main function that runs everything
- function to call the api, customizeable. 
- function to change the data format? 
- function to upload to gcs
do we save any of the calls to memory? 
- if we're dealing with parquet files we could use an in-process duck db, which could then upload directly to gcs. obvs duck db wouldn't scale out by itself, orchestrator would have to take care of that, but would be able to handle larger datasets than pandas (probs). so slight scale up vertical ability. I mean it's not like I'm using a distributed framework like spark.
- duckdb allows stuff to be done in sql, as opposed to pandas. 

- but then if you scaled out more you'd probably have an actual data layer that you could write to. sql is also more widely used by data teams. 

### development vs production
- we could use docker here? 
- or poetry? poetry and docker both more self-contained, good for testing env, but then code would go to github and then be pulled into where it runs properly.
- or just a good old fashioned python env, good for development. 



### coming from a go background
- integrated testing
- integrated benchmarking for speed, latency etc. 

### async vs sync
- sync easier to reason about
- async seems more natural for i/o bound tasks
- sync probably actually has higher throughput. basically throughput is all about how much nativie python code you've replaced, with c/c++. 
- in python multi-threading is co-operative, so threads have to yield to execution loop through await, async for, async with. so one thread can starve others of cpu time. but that's more about how you write your python code. 

### python specific things
- which libraries to use? 


### api part
- rate limiting
- pagination
- do we store things in memory? 
if number less than 0 is given, will default to 10 results. won't give more than 1000.

### security & general config
general assumption is that software should be agnostic of the platform that it runs on. to wit, it should expect the necessary credentials in place. 
google is all about adc, which it just looks for an the env. So as long as the current env is authenticated. it'll be fine. easiest way is to authenticate with the cli, but that does require your dev environment to have the cli. other ways to do it would be to have a credentials in the dir somewhere. 
then again, this is supposed to be done anonymously, so can just authenticate anonymously.
- use a config file for api urls, bucket names, creds? 
- sink logs to a source 


### error handling
- robustness
- idempotence. do we retry with backoff? 

### multiple extractions
- can we do lots of things at the same time? 

### efficiency
- how much time is lost? 
- how much storage does it need on the machine? 
- how much memory does it need on the machine? 

### scalability
- can we horizontally scale this out, would the machines be indifferent to each other. generally speaking designing systems for 2+ computers is much harder. Distributed computing. We'd then need an orchestrator. We could design our code so as to be a precursor to htis. 

### code organisation
 - do we bother modularizing, probs


if we actually think about the data flow here
get stuff from API. 
our call tells us what needs to go into the data set. 
output isn't deterministic. 

do work here? 
limited by i/o anyway

having in-memory database leads to some problems, what if everything goes down? 


but that would be the job of the orchestrator, don't need to address that here. 

what transformations are we doing? the id column is only useful in the context of making sure each json is processed. 

parquet would compress well, lots of strings