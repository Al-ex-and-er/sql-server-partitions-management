CREATE PROCEDURE testSSPM.Test_CopyTable_Heap1
as
set nocount on

CREATE TABLE TestHeap1(c1 int not null, c2 varchar(10))

exec sspm.CopyTableDefinition 'dbo.TestHeap1', 'TestHeap1_t' 
exec tSQLt.AssertEqualsTableSchema @Expected = 'TestHeap1', @actual = 'TestHeap1_t'

go
