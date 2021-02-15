--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE PROCEDURE testSSPM.Test_F_GetStopPID
as
set nocount on

CREATE PARTITION FUNCTION PF_TestGetStopPID_R (int) AS RANGE RIGHT FOR VALUES (10, 20)

CREATE PARTITION FUNCTION PF_TestGetStopPID_L (int) AS RANGE LEFT FOR VALUES (10, 20)

/*
drop table #expected
drop table #res_R
drop table #res_L
*/

CREATE TABLE #expected
(
  value int null,
  pid int null
)

CREATE TABLE #res_R
(
  value int null,
  pid int null
)

CREATE TABLE #res_L
(
  value int null,
  pid int null
)

INSERT INTO #expected VALUES
(5, null),
(10, 1),
(15, 1),
(20, 2),
(25, 2),
(null, 3)

INSERT INTO #res_R(value, pid)
SELECT value, sspm.GetStopPID(N'PF_TestGetStopPID_R', value)
FROM #expected

INSERT INTO #res_L(value, pid)
SELECT value, sspm.GetStopPID(N'PF_TestGetStopPID_L', value)
FROM #expected

/*
select * from #expected
select * from #res_R
select * from #res_L
*/

exec tSQLt.AssertEqualsTable '#expected', '#res_R', 'Error in #res_R'
exec tSQLt.AssertEqualsTable '#expected', '#res_L', 'Error in #res_L'

/*
DROP PARTITION FUNCTION PF_TestGetStopPID_R 

DROP PARTITION FUNCTION PF_TestGetStopPID_L
*/
go
