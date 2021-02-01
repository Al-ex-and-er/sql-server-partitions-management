
CREATE PARTITION FUNCTION PF_TestGetPIDRange_R (int) AS RANGE RIGHT FOR VALUES (10, 20)
CREATE PARTITION SCHEME PS_TestGetPIDRange_R AS PARTITION PF_TestGetPIDRange_R ALL TO ([PRIMARY])

--drop table dbo.TestDefrag2

CREATE TABLE dbo.TestDefrag2
(
  C1 int not null,
  UID uniqueidentifier default newid(),
  C2 nvarchar(200) not null,
  constraint PK_TestDefrag2 primary key(UID,  C1) 
  with (statistics_incremental = on)
  on PS_TestGetPIDRange_R(C1) 
)

CREATE NONCLUSTERED INDEX IX_TestDefrag2_C1 ON dbo.TestDefrag2(C2) 
with (statistics_incremental = on)
on PS_TestGetPIDRange_R(C1)

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


exec sspm.UpdateStats
  @TableName = 'dbo.TestDefrag2',
  @From = NULL,
  @To   = NULL,
  @MaxDOP = 1,
  @PKOnly = 0,
  @StatsNames = 'IX_TestDefrag2_C1, PK_TestDefrag2',
  @Debug = 1

