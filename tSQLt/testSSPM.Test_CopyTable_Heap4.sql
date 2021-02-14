--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE PROCEDURE testSSPM.Test_CopyTable_Heap4
as
set nocount on

CREATE PARTITION FUNCTION PF_TestHeap4 (int) AS RANGE RIGHT FOR VALUES (1, 2, 3)

CREATE PARTITION SCHEME PS_TestHeap4 AS PARTITION PF_TestHeap4 ALL TO ([PRIMARY])

CREATE TABLE TestHeap4(c1 int not null, c2 varchar(10)) on PS_TestHeap4(c1) with(data_compression = row) 

exec sspm.CopyTableDefinition 
  @SourceTable = 'dbo.TestHeap4', 
  @TargetTable = 'TestHeap4_t' ,
  @UsePS = 1

exec tSQLt.AssertEqualsTableSchema @Expected = 'TestHeap4', @actual = 'TestHeap4_t'

declare 
  @cmp nvarchar(60) = sspm.GetCompression('TestHeap4_t'),
  @ps sysname = sspm.GetPS('TestHeap4_t')

exec tSQLt.AssertEquals 'ROW', @cmp, 'wrong data_compression!'
exec tSQLt.AssertEquals 'PS_TestHeap4', @ps, 'wrong partition scheme!'

drop table TestHeap4_t

exec sspm.CopyTableDefinition 
  @SourceTable = 'dbo.TestHeap4', 
  @TargetTable = 'TestHeap4_t' ,
  @UsePS = 0

exec tSQLt.AssertEqualsTableSchema @Expected = 'TestHeap4', @actual = 'TestHeap4_t'

set @ps = sspm.GetPS('TestHeap4_t')

exec tSQLt.AssertEquals NULL, @ps, 'should be no partition scheme!'

go
