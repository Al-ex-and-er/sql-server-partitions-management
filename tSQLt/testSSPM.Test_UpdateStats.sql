CREATE OR ALTER PROCEDURE testSSPM.Test_UpdateStats
as
set nocount on

CREATE PARTITION FUNCTION PF_TestUpdateStats (int) AS RANGE RIGHT FOR VALUES (10, 20)
CREATE PARTITION SCHEME PS_TestUpdateStats AS PARTITION PF_TestUpdateStats ALL TO ([PRIMARY])

--drop table dbo.TestUpdateStats

CREATE TABLE dbo.TestUpdateStats
(
  C1 int not null,
  UID uniqueidentifier default newid(),
  C2 nvarchar(200) not null,
  constraint PK_TestUpdateStats primary key(UID,  C1) 
  with (statistics_incremental = on)
  on PS_TestUpdateStats(C1) 
)

CREATE NONCLUSTERED INDEX IX_TestUpdateStats_C1 ON dbo.TestUpdateStats(C2) 
with (statistics_incremental = on)
on PS_TestUpdateStats(C1)

declare @cnt int = 0

while @cnt < 2
begin
  INSERT INTO dbo.TestUpdateStats (C1, C2)
  SELECT top 1000
    C1 = abs(checksum(newid())) % 26,
    C2 = a.name + b.name
  FROM master.dbo.spt_values a
    cross join master.dbo.spt_values b
  WHERE a.name IS NOT NULL 
    and b.name IS NOT NULL
  ORDER BY NEWID()
  set @cnt += 1
end

exec sspm.UpdateStats
  @TableName = 'dbo.TestUpdateStats',
  @From = NULL,
  @To   = NULL,
  @PKOnly = 0,
  @StatsNames = 'IX_TestUpdateStats_C1, PK_TestUpdateStats',
  @Debug = 0

declare 
  @dt1 datetime = getdate(), 
  @dt2 datetime2

exec sspm.UpdateStats
  @TableName = 'dbo.TestUpdateStats',
  @From = 10,
  @To   = 20,
  @PKOnly = 1,
  @Debug = 0

set @dt2 = getdate()

DROP TABLE if exists #stats

CREATE TABLE #stats
(
  stats_id int not null,
  partition_number int not null,
  last_updated datetime2 not null
)

INSERT INTO #stats(stats_id, partition_number, last_updated)
SELECT sp.stats_id, sp.partition_number, sp.last_updated
FROM sys.stats s
  outer apply sys.dm_db_incremental_stats_properties(s.object_id, s.stats_id) sp
WHERE s.object_id = object_id('dbo.TestUpdateStats')

if not exists
(
  select 1 
  from #stats 
  where stats_id = 1 
    and last_updated between @dt1 and @dt2
    and partition_number = 2
)
begin
  exec tSQLt.Fail 'Partition #2 has not been updated!'
end

if exists
(
  select 1 
  from #stats
  where last_updated between @dt1 and @dt2
    and not (stats_id = 1 and partition_number = 2)
)
begin
  exec tSQLt.Fail 'Extra partitions have been updated!'
end


set @dt1 = getdate()

exec sspm.UpdateStats
  @TableName = 'dbo.TestUpdateStats',
  @From = NULL,
  @To   = NULL,
  @StatsNames = 'IX_TestUpdateStats_C1',
  @Debug = 0

set @dt2 = getdate()

TRUNCATE TABLE #stats

INSERT INTO #stats(stats_id, partition_number, last_updated)
SELECT sp.stats_id, sp.partition_number, sp.last_updated
FROM sys.stats s
  outer apply sys.dm_db_incremental_stats_properties(s.object_id, s.stats_id) sp
WHERE s.object_id = object_id('dbo.TestUpdateStats')

if 3 <>
  (
    select count(*) 
    from #stats 
    where stats_id = 2 
      and last_updated between @dt1 and @dt2
  )
begin
  exec tSQLt.Fail 'The index partitions have not been updated!'
end

if exists
(
  select 1 
  from #stats
  where last_updated between @dt1 and @dt2
    and not (stats_id = 2)
)
begin
  exec tSQLt.Fail 'Extra index partitions have been updated!'
end

DROP TABLE dbo.TestUpdateStats
DROP PARTITION SCHEME PS_TestUpdateStats
DROP PARTITION FUNCTION PF_TestUpdateStats

go