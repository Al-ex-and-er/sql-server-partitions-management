CREATE PROCEDURE testSSPM.Test_CopyTable_Heap2
as
set nocount on

CREATE TABLE TestHeap2(c1 int not null, c2 varchar(10)) with(data_compression = page)

exec sspm.CopyTableDefinition 'dbo.TestHeap2', 'TestHeap2_t' 
exec tSQLt.AssertEqualsTableSchema @Expected = 'TestHeap2', @actual = 'TestHeap2_t'

declare @cmp nvarchar(60) = sspm.GetCompression('TestHeap2_t')

exec tSQLt.AssertEquals 'PAGE', @cmp, 'wrong data_compression'
go
