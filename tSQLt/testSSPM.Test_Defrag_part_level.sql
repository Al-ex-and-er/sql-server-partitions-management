--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE or alter PROCEDURE testSSPM.Test_Defrag_part_level
as
set nocount on

CREATE PARTITION FUNCTION PF_TestGetPIDRange_R (int) AS RANGE RIGHT FOR VALUES (10, 20)
CREATE PARTITION SCHEME PS_TestGetPIDRange_R AS PARTITION PF_TestGetPIDRange_R ALL TO ([PRIMARY])

CREATE TABLE dbo.TestDefrag2
(
  C1 int not null,
  UID uniqueidentifier default newid(),
  C2 nvarchar(200) not null,
  constraint PK_TestDefrag2 primary key(UID,  C1) on PS_TestGetPIDRange_R(C1)
)

CREATE NONCLUSTERED INDEX IX_TestDefrag2_C1 ON dbo.TestDefrag2(C2) on PS_TestGetPIDRange_R(C1)

declare @cnt int = 0

while @cnt < 2
begin
  INSERT INTO dbo.TestDefrag2 (C1, C2)
  SELECT top 100000
    C1 = abs(checksum(newid())) % 26,
    C2 = a.name + b.name
  FROM master.dbo.spt_values a
    cross join master.dbo.spt_values b
  WHERE a.name IS NOT NULL 
    and b.name IS NOT NULL
  ORDER BY NEWID()
  set @cnt += 1
end

CREATE TABLE #log
(
  index_id int not null,
  pid int not null,
  action varchar(10) null,
  query nvarchar(max)
)

INSERT INTO #log
EXEC sspm.Defragment
  @TableName = 'dbo.TestDefrag2',
  @From = null,
  @To = 10,
  @PKOnly = 1,
  @Debug = 1

if 1 <> (select count(*) from #log)
  and 1 <> (select count(*) from #log where pid = 1)
begin
  exec tSQLt.Fail 'Only one the leftmost partition should be affected!'
end

TRUNCATE TABLE #log

INSERT INTO #log
EXEC sspm.Defragment
  @TableName = 'dbo.TestDefrag2',
  @From = 20,
  @To = null,
  @PKOnly = 1,
  @Debug = 1

if 1 <> (select count(*) from #log)
  and 1 <> (select count(*) from #log where pid = 3)
begin
  exec tSQLt.Fail 'Only one the rightmost partition should be affected!'
end

TRUNCATE TABLE #log

INSERT INTO #log
EXEC sspm.Defragment
  @TableName = 'dbo.TestDefrag2',
  @From = 10,
  @To = 20,
  @PKOnly = 1,
  @Debug = 1

if 1 <> (select count(*) from #log)
  and 1 <> (select count(*) from #log where pid = 2)
begin
  exec tSQLt.Fail 'Only partition #2 should be affected!'
end

TRUNCATE TABLE #log

INSERT INTO #log
EXEC sspm.Defragment
  @TableName = 'dbo.TestDefrag2',
  @From = null,
  @To = 30,
  @PKOnly = 1,
  @Debug = 1

if 2 <> (select count(*) from #log)
  and 2 <> (select count(*) from #log where pid in (1, 2))
begin
  exec tSQLt.Fail 'First two partitions should be affected!'
end

TRUNCATE TABLE #log

INSERT INTO #log
EXEC sspm.Defragment
  @TableName = 'dbo.TestDefrag2',
  @From = 10,
  @To = null,
  @PKOnly = 1,
  @Debug = 1

if 2 <> (select count(*) from #log)
  and 2 <> (select count(*) from #log where pid in (2, 3))
begin
  exec tSQLt.Fail 'Last two partitions should be affected!'
end

TRUNCATE TABLE #log

INSERT INTO #log
EXEC sspm.Defragment
  @TableName = 'dbo.TestDefrag2',
  @From = null,
  @To = null,
  @PKOnly = 1,
  @Debug = 1

if 3 <> (select count(*) from #log)
  and 3 <> (select count(*) from #log where pid in (1, 2, 3))
begin
  exec tSQLt.Fail 'All three partitions should be affected!'
end


declare @isOK bit = 0

begin try
  EXEC sspm.Defragment
    @TableName = 'dbo.TestDefrag2',
    @IndexName = 'wrong name',
    @Debug = 1
end try
begin catch
  set @isOK = 1
end catch

if @isOK = 0
  exec tSQLt.Fail 'proc should fail on not-existing index!'

/*
DROP TABLE dbo.TestDefrag2
DROP PARTITION SCHEME PS_TestGetPIDRange_R
DROP PARTITION FUNCTION PF_TestGetPIDRange_R
*/
go
