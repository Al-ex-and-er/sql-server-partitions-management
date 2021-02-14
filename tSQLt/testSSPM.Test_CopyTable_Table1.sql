--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE PROCEDURE testSSPM.Test_CopyTable_Table1
as
set nocount on

exec sspm.CopyTableDefinition
  @SourceTable = 'Warehouse.StockItemTransactions',
  @TargetTable = 'tmp_table',
  @UsePS = 0,
  @TargetPSName = null,
  @SkipIndexes = 0,
  @DropTargetIfExists = 1,
  @debug = 0

INSERT INTO tmp_table
SELECT top 100 * 
FROM Warehouse.StockItemTransactions

exec tSQLt.AssertEqualsTableSchema @Expected = 'Warehouse.StockItemTransactions', @actual = 'dbo.tmp_table'
go
