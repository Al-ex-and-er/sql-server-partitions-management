--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE PROCEDURE testSSPM.Test_F_GetPIDRange
as
set nocount on

CREATE PARTITION FUNCTION PF_TestGetPIDRange_R (int) AS RANGE RIGHT FOR VALUES (10, 20)
CREATE PARTITION FUNCTION PF_TestGetPIDRange_L (int) AS RANGE LEFT FOR VALUES (10, 20)

/*
DROP TABLE #expected
DROP TABLE #res
*/

CREATE TABLE #expected
(
  id char(4) not null,
  v1 int null,
  v2 int null,
  r_pid1 int null,
  r_pid2 int null,
  l_pid1 int null,
  l_pid2 int null
)

INSERT INTO #expected(id, v1, v2, r_pid1, r_pid2, l_pid1, l_pid2) VALUES
('__05', NULL, 5, NULL, NULL, NULL, NULL),
('__10', NULL, 10, 1, 1, 1, 1),
('__15', NULL, 15, 1, 1, 1, 1),
('__20', NULL, 20, 1, 2, 1, 2),
('__25', NULL, 25, 1, 2, 1, 2),
('____', NULL, NULL, 1, 3, 1, 3),
('0505', 5, 5, NULL, NULL, NULL, NULL),
('0510', 5, 10, NULL, NULL, NULL, NULL),
('0515', 5, 15, NULL, NULL, NULL, NULL),
('0520', 5, 20, 2, 2, 2, 2),
('0525', 5, 25, 2, 2, 2, 2),
('05__', 5, NULL, 2, 3, 2, 3),
('1010', 10, 10, NULL, NULL, NULL, NULL),
('1015', 10, 15, NULL, NULL, NULL, NULL),
('1020', 10, 20, 2, 2, 2, 2),
('1025', 10, 25, 2, 2, 2, 2),
('10__', 10, NULL, 2, 3, 2, 3),
('1515', 15, 15, NULL, NULL, NULL, NULL),
('1520', 15, 20, NULL, NULL, NULL, NULL),
('1525', 15, 25, NULL, NULL, NULL, NULL),
('15__', 15, NULL, 3, 3, 3, 3),
('2020', 20, 20, NULL, NULL, NULL, NULL),
('2025', 20, 25, NULL, NULL, NULL, NULL),
('20__', 20, NULL, 3, 3, 3, 3),
('2525', 25, 25, NULL, NULL, NULL, NULL),
('25__', 25, NULL, NULL, NULL, NULL, NULL)

CREATE TABLE #res
(
  id char(4) not null,
  v1 int null,
  v2 int null,
  r_pid1 int null,
  r_pid2 int null,
  l_pid1 int null,
  l_pid2 int null
)

INSERT INTO #res(id, v1, v2, r_pid1, r_pid2, l_pid1, l_pid2)
SELECT 
  id = isnull(right('0'+cast(t.v1 as varchar(10)), 2), '__') + isnull(right('0'+cast(v2 as varchar(10)), 2), '__'),
  v1,
  v2,
  r_pid1 = r.pid1, 
  r_pid2 = r.pid2,
  l_pid1 = l.pid1, 
  l_pid2 = l.pid2
FROM #expected t
  outer apply sspm.GetPIDRange(N'PF_TestGetPIDRange_R', v1, v2) r
  outer apply sspm.GetPIDRange(N'PF_TestGetPIDRange_L', v1, v2) l


/*
select * from #expected
select * from #res

select *
from #expected e
  join #res r
    on r.id = e.id
where isnull(e.r_pid1, -1) <> isnull(r.r_pid1, -1)
   or isnull(e.r_pid2, -1) <> isnull(r.r_pid2, -1)
   or isnull(e.l_pid1, -1) <> isnull(r.l_pid1, -1)
   or isnull(e.l_pid2, -1) <> isnull(r.l_pid2, -1)
*/

exec tSQLt.AssertEqualsTable '#expected', '#res'

/*
DROP PARTITION FUNCTION PF_TestGetPIDRange_R 
DROP PARTITION FUNCTION PF_TestGetPIDRange_L
*/
go
