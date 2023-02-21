## Week 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 


### Log Outputs
* For yellow and green data trips
```
12:27:26  Found 7 models, 10 tests, 0 snapshots, 0 analyses, 452 macros, 0 operations, 1 seed file, 8 sources, 0 exposures, 0 metrics
12:27:26  
12:27:27  Concurrency: 1 threads (target='dev')
12:27:27  
12:27:27  1 of 3 START sql table model dbo.dim_zones ..................................... [RUN]
12:27:30  1 of 3 OK created sql table model dbo.dim_zones ................................ [OK in 2.73s]
12:27:30  2 of 3 START sql view model dbo.stg_fhv_tripdata ............................... [RUN]
12:27:30  2 of 3 OK created sql view model dbo.stg_fhv_tripdata .......................... [OK in 0.42s]
12:27:30  3 of 3 START sql table model dbo.fact_fhv_trips ................................ [RUN]
12:27:38  3 of 3 ERROR creating sql table model dbo.fact_fhv_trips ....................... [ERROR in 7.88s]
12:27:38
12:27:38  Finished running 2 table models, 1 view model in 0 hours 0 minutes and 12.35 seconds (12.35s).
```

** User Notes **
1. Microsoft Power BI is used instead of Google Data Studio for dashboard creation to create seamless integration of Azure services.

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)** 

You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- [ ] 41648442
- [ ] 51648442
- [x] 61648442
- [ ] 71648442

Answer is 61585449 (not in choices closest 61648442)

![](resources/images/2023-02-20-00-24-05.png)

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 

- [x] 89.9/10.1
- [ ] 94/6
- [ ] 76.3/23.7
- [ ] 99.1/0.9

![](resources/images/2023-02-21-16-09-42.png)

### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- [ ] 33244696
- [x] 43244696
- [ ] 53244696
- [ ] 63244696

![](resources/images/2023-02-20-22-45-24.png)

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  
 
Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- [ ] 12998722
- [x] 22998722
- [ ] 32998722
- [ ] 42998722

![](resources/images/2023-02-20-23-09-38.png)

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- [ ] March
- [ ] April
- [x] January
- [ ] December

![](resources/images/2023-02-21-15-34-52.png)


## Submitting the solutions

* Form for submitting: https://forms.gle/6A94GPutZJTuT5Y16
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 25 February (Saturday), 22:00 CET


## Solution

We will publish the solution here