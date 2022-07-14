--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE PROCEDURE testSSPM.Test_F_GetValueByPID
as
set nocount on

CREATE PARTITION FUNCTION PF_TestGetValueByPID_R (int) AS RANGE RIGHT FOR VALUES (10, 20, 30)

CREATE PARTITION FUNCTION PF_TestGetValueByPID_L (int) AS RANGE LEFT FOR VALUES (10, 20, 30)

DROP TABLE IF EXISTS #res

CREATE TABLE #res
(
  PID int not null,
  result int null
)

INSERT INTO #res VALUES
(1, NULL),
(2, 10),
(3, 20),
(4, 30),
(5, NULL)

if exists
(
  SELECT 1
  FROM #res r
  WHERE
    isnull(r.result, -1) <> isnull(sspm.GetValueByPID(N'PF_TestGetValueByPID_R', r.PID), -1)
)
begin
  exec tSQLt.Fail 'PF_TestGetValueByPID_R failed validation'
end

TRUNCATE TABLE #res

INSERT INTO #res VALUES
(1, 10),
(2, 20),
(3, 30),
(4, NULL)

if exists
(
  SELECT 1
  FROM #res r
  WHERE
    isnull(r.result, -1) <> isnull(sspm.GetValueByPID(N'PF_TestGetValueByPID_L', r.PID), -1)
)
begin
  exec tSQLt.Fail 'PF_TestGetValueByPID_L failed validation'
end


/*
DROP PARTITION FUNCTION PF_TestGetValueByPID_R 

DROP PARTITION FUNCTION PF_TestGetValueByPID_L
*/
go
