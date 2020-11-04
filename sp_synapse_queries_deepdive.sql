

CREATE PROC sp_synapse_queries_deepdive (@request_id varchar(20), @distributions bit, @tempdb bit)
AS


PRINT ' __        ___   _    _  _____      _    __  __   ___   _     ___   ___  _  _____ _   _  ____      _  _____ ___ '
PRINT ' \ \      / / | | |  / \|_   _|    / \  |  \/  | |_ _| | |   / _ \ / _ \| |/ /_ _| \ | |/ ___|    / \|_   _|__ \'
PRINT '  \ \ /\ / /| |_| | / _ \ | |     / _ \ | |\/| |  | |  | |  | | | | | | |   / | ||  \| | |  _    / _ \ | |   / /'
PRINT '   \ V  V / |  _  |/ ___ \| |    / ___ \| |  | |  | |  | |__| |_| | |_| | . \ | || |\  | |_| |  / ___ \| |  |_| '
PRINT '    \_/\_/  |_| |_/_/   \_\_|   /_/   \_\_|  |_| |___| |_____\___/ \___/|_|\_\___|_| \_|\____| /_/   \_\_|  (_) '


PRINT 'READ MORE AT: https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-manage-monitor'
                                                                                                                

PRINT '-> QUERY STEPS/PLAN'
PRINT '-> LOCK ESCALATION'
IF @distributions = 1
	PRINT '-> QUERY STEPS ON ALL DISTRIBUTED DATABASES'
IF @distributions = 1
	PRINT '-> DATA MOVEMENT STEPS ON EACH DISTRIBUTION'
IF @tempdb = 1
	PRINT '-> TEMPDB INFO'


SELECT	*
FROM	sys.dm_pdw_request_steps 
WHERE	request_id = @request_id
ORDER BY step_index;

SELECT waits.session_id,
      waits.request_id,  
      requests.command,
      requests.status,
      requests.start_time,  
      waits.type,
      waits.state,
      waits.object_type,
      waits.object_name
FROM   sys.dm_pdw_waits waits
   JOIN  sys.dm_pdw_exec_requests requests
   ON waits.request_id=requests.request_id
WHERE waits.request_id = @request_id
ORDER BY waits.object_name, waits.object_type, waits.state;

IF @distributions = 1
BEGIN
	SELECT * FROM sys.dm_pdw_sql_requests
	WHERE request_id = @request_id AND step_index = 2;

	SELECT * FROM sys.dm_pdw_dms_workers
	WHERE request_id = @request_id AND step_index = 2;
END

IF @tempdb = 1
BEGIN
SELECT
    sr.request_id,
    ssu.session_id,
    ssu.pdw_node_id,
    sr.command,
    sr.total_elapsed_time,
    es.login_name AS 'LoginName',
    DB_NAME(ssu.database_id) AS 'DatabaseName',
    (es.memory_usage * 8) AS 'MemoryUsage (in KB)',
    (ssu.user_objects_alloc_page_count * 8) AS 'Space Allocated For User Objects (in KB)',
    (ssu.user_objects_dealloc_page_count * 8) AS 'Space Deallocated For User Objects (in KB)',
    (ssu.internal_objects_alloc_page_count * 8) AS 'Space Allocated For Internal Objects (in KB)',
    (ssu.internal_objects_dealloc_page_count * 8) AS 'Space Deallocated For Internal Objects (in KB)',
    CASE es.is_user_process
    WHEN 1 THEN 'User Session'
    WHEN 0 THEN 'System Session'
    END AS 'SessionType',
    es.row_count AS 'RowCount'
FROM sys.dm_pdw_nodes_db_session_space_usage AS ssu
    INNER JOIN sys.dm_pdw_nodes_exec_sessions AS es ON ssu.session_id = es.session_id AND ssu.pdw_node_id = es.pdw_node_id
    INNER JOIN sys.dm_pdw_nodes_exec_connections AS er ON ssu.session_id = er.session_id AND ssu.pdw_node_id = er.pdw_node_id
    INNER JOIN microsoft.vw_sql_requests AS sr ON ssu.session_id = sr.spid AND ssu.pdw_node_id = sr.pdw_node_id
WHERE DB_NAME(ssu.database_id) = 'tempdb'
    AND es.session_id <> @@SPID
    AND es.login_name <> 'sa'
	AND	sr.request_id = @request_id
ORDER BY sr.request_id;
END