CREATE PROCEDURE testSSPM.Test_F_GetStartPID
as
set nocount on

CREATE PARTITION FUNCTION PF_TestGetStartPID_R (int) AS RANGE RIGHT FOR VALUES (10, 20)

CREATE PARTITION FUNCTION PF_TestGetStartPID_L (int) AS RANGE LEFT FOR VALUES (10, 20)

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
(null, 1),
(5, 2),
(10, 2),
(15, 3),
(20, 3),
(25, null)

INSERT INTO #res_R(value, pid)
SELECT value, sspm.GetStartPID(N'PF_TestGetStartPID_R', value)
FROM #expected

INSERT INTO #res_L(value, pid)
SELECT value, sspm.GetStartPID(N'PF_TestGetStartPID_L', value)
FROM #expected

/*
select * from #expected
select * from #res_R
select * from #res_L
*/

exec tSQLt.AssertEqualsTable '#expected', '#res_R', 'Error in #res_R'
exec tSQLt.AssertEqualsTable '#expected', '#res_L', 'Error in #res_L'

/*
DROP PARTITION FUNCTION PF_TestGetStartPID_R 

DROP PARTITION FUNCTION PF_TestGetStartPID_L
*/
go
