CREATE PROCEDURE testSSPM.Test_CopyTable_Heap3
as
set nocount on

CREATE TABLE TestHeap3(c1 int not null, c2 varchar(10)) on [PRIMARY] with(data_compression = page) 

exec sspm.CopyTableDefinition 'dbo.TestHeap3', 'TestHeap3_t' 

exec tSQLt.AssertEqualsTableSchema @Expected = 'TestHeap3', @actual = 'TestHeap3_t'

declare 
  @cmp nvarchar(60) = sspm.GetCompression('TestHeap3_t'),
  @fg sysname = sspm.GetFG('TestHeap3_t')

exec tSQLt.AssertEquals 'PAGE', @cmp, 'wrong data_compression!'
exec tSQLt.AssertEquals 'PRIMARY', @fg, 'wrong filegroup!'
go
