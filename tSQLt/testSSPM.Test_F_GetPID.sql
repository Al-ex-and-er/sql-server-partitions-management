--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE PROCEDURE testSSPM.Test_F_GetPID
as
set nocount on

CREATE PARTITION FUNCTION PF_TestGetPID_R (int) AS RANGE RIGHT FOR VALUES (10, 20, 30)
CREATE PARTITION SCHEME PS_TestGetPID_R AS PARTITION PF_TestGetPID_R ALL TO ([PRIMARY])

CREATE TABLE TestGetPID_R(c1 int not null, c2 varchar(10)) on PS_TestGetPID_R(c1) 

INSERT INTO TestGetPID_R VALUES
( 5, 'A'),
(10, 'B'),
(15, 'C'),
(20, 'D'),
(25, 'E')

CREATE PARTITION FUNCTION PF_TestGetPID_L (int) AS RANGE LEFT FOR VALUES (10, 20, 30)
CREATE PARTITION SCHEME PS_TestGetPID_L AS PARTITION PF_TestGetPID_L ALL TO ([PRIMARY])

CREATE TABLE TestGetPID_L(c1 int not null, c2 varchar(10)) on PS_TestGetPID_L(c1) 

INSERT INTO TestGetPID_L VALUES
( 5, 'A'),
(10, 'B'),
(15, 'C'),
(20, 'D'),
(25, 'E')

if exists
(
  SELECT 1
  FROM (values(5), (10), (15), (20), (25))t(i)
    cross apply 
    ( SELECT
        r1 = sspm.GetPID(N'PF_TestGetPID_R', i), 
        r2 = $partition.PF_TestGetPID_R(i)
    )p
  WHERE
    p.r1 <> p.r2
)
begin
  exec tSQLt.Fail 'PF_TestGetPID_R failed validation'
end

if exists
(
  SELECT 1
  FROM (values(5), (10), (15), (20), (25))t(i)
    cross apply 
    ( SELECT
        r1 = sspm.GetPID(N'PF_TestGetPID_L', i), 
        r2 = $partition.PF_TestGetPID_L(i)
    )p
  WHERE
    p.r1 <> p.r2
)
begin
  exec tSQLt.Fail 'PF_TestGetPID_L failed validation'
end

/*
DROP TABLE TestGetPID_R
DROP PARTITION SCHEME PS_TestGetPID_R 
DROP PARTITION FUNCTION PF_TestGetPID_R 

DROP TABLE TestGetPID_L
DROP PARTITION SCHEME PS_TestGetPID_L
DROP PARTITION FUNCTION PF_TestGetPID_L
*/
go
