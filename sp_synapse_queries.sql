
CREATE PROC [dbo].[sp_whoisactive] AS

IF EXISTS (SELECT * FROM sys.dm_pdw_exec_requests WHERE status = 'Suspended' AND session_id <> session_id())
BEGIN

	SELECT    DISTINCT s.session_id,
			s.request_id,
			--s.login_time,
			s.login_name,
			a.status,
			DATEDIFF(MINUTE,a.start_time,GETDATE()) AS running_time	,
			DATEDIFF(MINUTE,a.submit_time,a.start_time) AS time_in_queue	,
			DATEADD(HOUR,2,a.submit_time		) AS submit_time		,
			DATEADD(HOUR,2,a.start_time			) AS start_time			,
			DATEADD(HOUR,2,a.end_compile_time	) AS end_compile_time	,
			DATEADD(HOUR,2,a.end_time			) AS end_time			,
			a.[label],
			a.command,
			l.blocking_session_id,
			s.app_name,
			A.resource_class,
		CONCAT('EXEC dbo.sp_whoisactive_deepdive @request_id = ''',s.request_id,''', @distributions = 0, @tempdb = 0') deepdive
			--SELECT COUNT(*)
	FROM    sys.dm_pdw_exec_sessions s
	LEFT JOIN sys.dm_pdw_exec_requests a ON        s.request_id = a.request_id
	LEFT JOIN (
				SELECT  t1.request_session_id,
						t2.blocking_session_id
				FROM sys.dm_pdw_nodes_tran_locks as t1
				LEFT JOIN sys.dm_pdw_nodes_os_waiting_tasks as t2
				ON t1.lock_owner_address = t2.resource_address
			) l
	ON        l.request_session_id = s.sql_spid
	WHERE    a.status IN ('Suspended')
	AND		s.session_id <> Session_id() 
	ORDER BY start_time DESC

END

	SELECT    DISTINCT s.session_id,
			s.request_id,
			--s.login_time,
			s.login_name,
			a.status,
			DATEDIFF(MINUTE,a.start_time,GETDATE()) AS running_time	,
			DATEDIFF(MINUTE,a.submit_time,a.start_time) AS time_in_queue	,
			DATEADD(HOUR,2,a.submit_time		) AS submit_time		,
			DATEADD(HOUR,2,a.start_time			) AS start_time			,
			DATEADD(HOUR,2,a.end_compile_time	) AS end_compile_time	,
			DATEADD(HOUR,2,a.end_time			) AS end_time			,
			a.[label],
			a.command,
			l.blocking_session_id,
			s.app_name,
			A.resource_class,
		CONCAT('EXEC dbo.sp_whoisactive_deepdive @request_id = ''',s.request_id,''', @distributions = 0, @tempdb = 0') deepdive
			--SELECT COUNT(*)
	FROM    sys.dm_pdw_exec_sessions s
	LEFT JOIN sys.dm_pdw_exec_requests a ON        s.request_id = a.request_id
	LEFT JOIN (
				SELECT  t1.request_session_id,
						t2.blocking_session_id
				FROM sys.dm_pdw_nodes_tran_locks as t1
				LEFT JOIN sys.dm_pdw_nodes_os_waiting_tasks as t2
				ON t1.lock_owner_address = t2.resource_address
			) l
	ON        l.request_session_id = s.sql_spid
	WHERE    a.status IN ('Running')
	AND		s.session_id <> Session_id() 
	ORDER BY start_time DESC

