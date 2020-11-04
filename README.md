# sp_synapse_queries

## Desciption

## Permissions Required

GRANT VIEW DATABASE STATE
GRANT EXECUTE ON sp_synapse_queries
GRANT EXECUTE ON sp_synapse_queries_deepdive

## Files

## How to Use
So lets get started, where should you begin? That is relatively simple, all you need to do is run the below SQL command. Again, this will only work as explained on zajbppdw01.

```sql
EXEC dbo.sp_whoisactive;
```

Ok great, what now. Well now I explain (will be shortened due to me not enjoying documentation), you will get the below output. The first results set is all the queries in the queue (YES THAT MEANS IT IS NOT DOING ANYTHING), the second result set is all the queries that is actually running at the moment.


|Column Name	|JP’s Description|
| ------------ | ------------ |
|session_id	|Unique session id for a query|
|request_id	|Unique Query id in a session|
|login_name	|Who is running this session|
|status	|Is the query running or in a queue|
|running_time	|How long in seconds has the query been running for|
|time_in_queue	|How long was the query in a queue before it started executing|
|submit_time	|When did you press F5|
|start_time	|When did it actually start|
|end_compile_time	|When did it finish creating the execution plan|
|end_time	|When did your query finish|
|label	|Yes you guessed it|
|command	|SQL Command that is being executed|
|blocking_session_id	|If populated something is blocking your query|
|app_name	|Application Session is coming from|
|resource_class	|Resource Class, you can read up on this|
|deepdive	|Magic statement we will be explaining|

Yes I knew you would not be able to stop thinking about the magic command, so what is it? Well essentially it is just another stored procedure called sp_whoisactive_deepdive prepopulated with the basic deep dive parameters

```sql
EXEC dbo.sp_whoisactive_deepdive @request_id = 'QID10741526', @distributions = 0, @tempdb = 0
```

If you run it as is, you will get 2 more result sets as below.
1.	The Query Steps, a nice way of looking at what step your query is busy with
2.	Waits, what your query has locks on and what it could be waiting on

If you want to dive even deeper, you can set the @distributions parameter to 1, this will then produce the below.
1.	The Query Steps on all distribution Nodes
2.	The Data Movement steps your query is busy with

Now when you are feeling adventurous turn on the @tempdb parameter and you will see what you query is doing to TempDB on Synapse.

