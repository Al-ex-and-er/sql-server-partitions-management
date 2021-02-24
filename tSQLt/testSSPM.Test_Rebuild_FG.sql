--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE or alter PROCEDURE testSSPM.Test_Rebuild_FG
as
set nocount on

ALTER DATABASE sspm ADD FILEGROUP TestFG

declare @filename sysname = cast(serverproperty('InstanceDefaultDataPath') as sysname) + 'TestFG.ndf'
declare @sql nvarchar(2048) = 'ALTER DATABASE sspm ADD FILE (NAME = sspmTestFG, FILENAME = ''' + @filename + ''') TO FILEGROUP TestFG'

exec(@sql)

CREATE PARTITION FUNCTION PF_TestDefragFG_R (int) AS RANGE RIGHT FOR VALUES (10, 20)
CREATE PARTITION SCHEME PS_TestDefragFG_R AS PARTITION PF_TestDefragFG_R ALL TO (TestFG)

CREATE TABLE dbo.TestDefragFG1
(
  C1 int not null,
  UID uniqueidentifier default newid(),
  C2 nvarchar(200) not null,
  constraint PK_TestDefragFG1 primary key(UID,  C1) on PS_TestDefragFG_R(C1)
)

CREATE NONCLUSTERED INDEX IX_TestDefragFG1_C1 ON dbo.TestDefragFG1(C2) on PS_TestDefragFG_R(C1)

declare @cnt int = 0

while @cnt < 2
begin
  INSERT INTO dbo.TestDefragFG1 (C1, C2)
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
EXEC sspm.RebuildFG
  @FGName = 'TestFG',
  @From = null,
  @To = null,
  @PKOnly = 0,
  @Debug = 1

if 6 <> (select count(*) from #log)
begin
  exec tSQLt.Fail '6 partitions on 2 indexes expected!'
end

/*
DROP TABLE dbo.TestDefragFG1
DROP PARTITION SCHEME PS_TestDefragFG_R
DROP PARTITION FUNCTION PF_TestDefragFG_R
ALTER DATABASE sspm REMOVE FILE sspmTestFG
ALTER DATABASE sspm REMOVE FILEGROUP TestFG
*/
go
