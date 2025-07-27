#### Introduction
This is a simple program to get some data from an API and push it to GCS. 
There are two services here:
- The ingest service collects data asychonrously from the faker api. It then collates that data and periodically pushes to a duckdb table. This runs to completion. 
- The export service saves a snapshot of the duckdb table, and exports to a google cloud bucket.

#### How to Run
`docker compose up`

#### A Note on Structure
Looking back on it this was definitely over-engineered and not done in a great way but I was kind of interested in the in-process duckdb for its own sake and also very time-pressured. 

Thinking from right to left, I wanted the final product to be in parquet such that downstream it's much easier to put into BigQuery; its self-defined schema makes loading easier. Parquet also compresses nicely, especially if the api data is full of strings. So, needing a way to deal with parquet data, I settled on duckdb. This adds an extra layer of complexity and has some overlap with pandas but it's just about worth it as:
- Duckdb can hold more data in memory than pandas.
- Introducing a db means that if the final export fails for any reason, data isn't lost.
- I find it easier to reason about transformations in sql than chaining pandas calls.

However, it's worth having pandas as well, as that's good for in-memory data manipulation i.e. taking the json and turning it into a structured form. If you scaled out more/wanted to do more transformation you'd probably have an actual data ingestion layer you'd write to before exporting to GCS but hey ho. 

Then, for the API part as this is an I/O bound task, doing it asynchronously makes it a bit easier to reason about. I haven't touched async in a while but hopefully the logic is fine. The code basically puts a load of tasks on the event loop, but instead of waiting for them all to finish (could use loads of memory), we grab them as they're done, batch 'em up and push to the db. Instead of actually implementing rate limiting or any fancy API stuff I'm using sleep and a semaphore to limit throughput. Also, as the output of api calls isn't deterministic, it didn't make much sense to have re-try functionality here. 










