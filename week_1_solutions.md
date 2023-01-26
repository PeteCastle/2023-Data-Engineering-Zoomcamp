# Week 1 Homework Solutions


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- [ ] `--imageid string`
- [x] `--iidfile string`
- [ ] `--idimage string`
- [ ] `--idfile string`

![](resources/images/2023-01-26-11-55-57.png)

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- [ ] 1
- [ ] 6
- [x] 3
- [ ] 7

![](resources/images/2023-01-26-11-59-15.png)

## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- [ ] 20689
- [x] 20530
- [ ] 17630
- [ ] 21090

```
SELECT COUNT(*) FROM green_taxi_trips 
    WHERE date(lpep_pickup_datetime) = CAST('2019-01-15' AS DATE) 
    AND date(lpep_dropoff_datetime) = CAST('2019-01-15' AS DATE)
```

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- [ ] 2019-01-18
- [ ] 2019-01-28
- [X] 2019-01-15
- [ ] 2019-01-10

```
SELECT DATE(lpep_pickup_datetime),MAX(trip_distance) AS total_distance  
	FROM green_taxi_trips
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY total_distance DESC
```
![](resources/images/2023-01-26-12-17-01.png)

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- [ ] 2: 1282 ; 3: 266
- [ ] 2: 1532 ; 3: 126
- [x] 2: 1282 ; 3: 254
- [ ] 2: 1282 ; 3: 274
```
SELECT (SELECT COUNT(*) 
	FROM green_taxi_trips
WHERE DATE(lpep_pickup_datetime) = CAST('2019-01-01' AS DATE)
	AND passenger_count = 2) AS two_psgrs,
(SELECT COUNT(*) 
	FROM green_taxi_trips
WHERE DATE(lpep_pickup_datetime) = CAST('2019-01-01' AS DATE)
	AND passenger_count = 3) AS three_psgrs
```
![](resources/images/2023-01-26-12-22-19.png)

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- [ ] Central Park
- [ ] Jamaica
- [ ] South Ozone Park
- [x] Long Island City/Queens Plaza

```
SELECT gtt."DOLocationID", tz."Zone", MAX(gtt.tip_amount) AS largest_tip
	FROM green_taxi_trips AS gtt
	LEFT JOIN taxi_zones AS tz
	ON gtt."DOLocationID" = tz."LocationID"
	WHERE "PULocationID" = (SELECT "LocationID" FROM taxi_zones WHERE "Zone" = 'Astoria')
	GROUP BY gtt."DOLocationID", tz."Zone"
	ORDER BY largest_tip DESC
```
![](resources/images/2023-01-26-13-38-22.png)


