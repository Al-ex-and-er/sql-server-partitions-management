CREATE PROCEDURE testSSPM.Test_Switch
as
set nocount on

CREATE PARTITION FUNCTION PF_Switch_R (int) AS RANGE RIGHT FOR VALUES (10, 20)
CREATE PARTITION SCHEME PS_Switch_R AS PARTITION PF_Switch_R ALL TO ([PRIMARY])

CREATE TABLE dbo.Test_Switch_R(c1 int not null, c2 varchar(10)) on PS_Switch_R(c1) 

INSERT INTO dbo.Test_Switch_R VALUES
( 5, 'A'),
(10, 'B'),
(15, 'C'),
(20, 'D'),
(25, 'E')

CREATE TABLE dbo.Test_Switch_Temp_R(c1 int not null, c2 varchar(10)) on PS_Switch_R(c1) 

CREATE TABLE #expected
(
  id char(4) not null primary key,
  v1 int null,
  v2 int null,
  pid1 int null,
  pid2 int null
)

INSERT INTO #expected(id, v1, v2, pid1, pid2) VALUES
('__05', NULL,    5, NULL, NULL),
('__10', NULL,   10,    1,    1),
('__15', NULL,   15,    1,    1),
('__20', NULL,   20,    1,    2),
('__25', NULL,   25,    1,    2),
('____', NULL, NULL,    1,    3),
('0505',    5,    5, NULL, NULL),
('0510',    5,   10, NULL, NULL),
('0515',    5,   15, NULL, NULL),
('0520',    5,   20,    2,    2),
('0525',    5,   25,    2,    2),
('05__',    5, NULL,    2,    3),
('1010',   10,   10, NULL, NULL),
('1015',   10,   15, NULL, NULL),
('1020',   10,   20,    2,    2),
('1025',   10,   25,    2,    2),
('10__',   10, NULL,    2,    3),
('1515',   15,   15, NULL, NULL),
('1520',   15,   20, NULL, NULL),
('1525',   15,   25, NULL, NULL),
('15__',   15, NULL,    3,    3),
('2020',   20,   20, NULL, NULL),
('2025',   20,   25, NULL, NULL),
('20__',   20, NULL,    3,    3),
('2525',   25,   25, NULL, NULL),
('25__',   25, NULL, NULL, NULL)




declare @RowsInserted bigint, @RowsDeleted bigint

--switch out one partition
exec sspm.Switch
  @SourceTable = 'dbo.Test_Switch_R',
  @TargetTable = 'dbo.Test_Switch_Temp_R',
  @From = NULL,
  @To = 10,
  @SkipCount = 0,
  @RowsInserted = @RowsInserted out,
  @RowsDeleted = @RowsDeleted out,
  @Debug = 1

--select @RowsInserted, @RowsDeleted
if @RowsInserted <> 1
begin
  exec tSQLt.Fail '@RowsInserted <> 1!'
end

if 1 <> (select count(*) from dbo.Test_Switch_Temp_R)
  or not exists (select 1 from dbo.Test_Switch_Temp_R where c1 = 5)
begin
  exec tSQLt.Fail 'dbo.Test_Switch_Temp_R must contain only one row, c1 = 5!'
end

if exists (select 1 from dbo.Test_Switch_R where c1 = 5)
begin
  exec tSQLt.Fail 'dbo.Test_Switch_R should not contain c1 = 5!'
end

--switch out one partition back
--declare @RowsInserted bigint, @RowsDeleted bigint

exec sspm.Switch
  @SourceTable = 'dbo.Test_Switch_Temp_R',
  @TargetTable = 'dbo.Test_Switch_R',
  @From = NULL,
  @To = 10,
  @SkipCount = 0,
  @RowsInserted = @RowsInserted out,
  @RowsDeleted = @RowsDeleted out,
  @Debug = 1

--select @RowsInserted, @RowsDeleted

if @RowsInserted <> 1
begin
  exec tSQLt.Fail '@RowsInserted <> 1!'
end

if 5 <> (select count(*) from dbo.Test_Switch_R)
  or not exists (select 1 from dbo.Test_Switch_R where c1 = 5)
begin
  exec tSQLt.Fail 'dbo.Test_Switch_R must contain row c1 = 5!'
end

if exists (select 1 from dbo.Test_Switch_Temp_R where c1 = 5)
begin
  exec tSQLt.Fail 'dbo.Test_Switch_Temp_R should not contain c1 = 5!'
end


CREATE PARTITION FUNCTION PF_Switch_L (int) AS RANGE LEFT FOR VALUES (10, 20)
CREATE PARTITION SCHEME PS_Switch_L AS PARTITION PF_Switch_L ALL TO ([PRIMARY])

CREATE TABLE dbo.Test_Switch_L(c1 int not null, c2 varchar(10)) on PS_TestL(c1) 

INSERT INTO dbo.Test_Switch_L VALUES
( 5, 'A'),
(10, 'B'),
(15, 'C'),
(20, 'D'),
(25, 'E')

CREATE TABLE dbo.Test_Switch_Temp_L(c1 int not null, c2 varchar(10)) on PS_Switch_L(c1) 

/*
DROP TABLE dbo.Test_Switch_Temp_R
DROP TABLE dbo.Test_Switch_R
DROP PARTITION SCHEME PS_Switch_R
DROP PARTITION FUNCTION PF_Switch_R

DROP TABLE dbo.Test_Switch_Temp_L
DROP TABLE dbo.Test_Switch_L
DROP PARTITION SCHEME PS_Switch_L
DROP PARTITION FUNCTION PF_Switch_L
*/
go
