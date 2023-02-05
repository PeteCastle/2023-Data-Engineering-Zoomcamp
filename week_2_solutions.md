## Week 2 Homework


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

[*] 447,770
[ ] 766,792
[ ] 299,234
[ ] 822,132
![Week 2 Question 1 Screenshot](resources/images/2023-02-05-20-24-44.png)

## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

[*] `0 5 1 * *`
[ ] `0 0 5 1 *`
[ ] `5 * 1 0 *`
[ ] `* * 5 1 0`
  
![Week 2 Question 2 Screenshot](resources/images/2023-02-05-20-26-32.png)

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

[*] 14,851,920
[ ] 12,282,990
[ ] 27,235,753
[ ] 11,338,483

Note:  I am using Azure Synapse DB instead of BigQuery.  There are minor modifications in the code, but the ETL process is still the same.  The code is in ./flows/parameterized_synapse.py

Tasks runs for each of the two datsets:

![Week 2 Question 3 Screenshot](resources/images/2023-02-05-22-10-34.png)

Output:
```
citus=> SELECT COUNT(*) FROM "combined_dataframe";
  count   
----------
 14851920
(1 row)
```


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

[ ] 88,019
[ ] 192,297
[*] 88,605
[ ] 190,225

Code used:
`prefect deployment build -sb="github/github-block" ./flows/parameterized_flow.py:etl_parent_flow -n "parameterized-flow-github-version"`

`prefect deployment apply etl_parent_flow-deployment.yaml`

Notes:
* When you specify github block for storage `-sb`, your path to flow is NOT local path but path from the root of your repo. 
* The storage block `github-block` is created from the Github Block.

![Week 2 Question 4 Screenshot](resources/images/2023-02-05-22-03-41.png)

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

[ ] `125,268`
[ ] `377,922`
[ ] `728,390`
[*] `514,392`

Proof of Flow Run:
![Week 2 Question 5 Screenshot](resources/images/2023-02-05-22-24-35.png)

Notification in Slack:

![Week 2 Question 5 Screenshot](resources/images/2023-02-05-22-25-24.png)

Message [link](https://temp-notify.slack.com/archives/C04M4NAM67L/p1675606901111839)

## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

[ ] 5
[ ] 6
[*] 8
[ ] 10

![Week 2 Question 6 Screenshot](resources/images/2023-02-05-20-51-25.png)
