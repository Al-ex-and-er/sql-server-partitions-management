ALTER PROCEDURE testSSPM.TestAllTables
as

CREATE TABLE #tables 
(
  id int identity(1,1) not null,
  tname sysname not null
)

INSERT INTO #tables (tname)
SELECT schema_name(schema_id) + '.' + name
FROM sys.tables 
--where SCHEMA_NAME(schema_id) <> 'tSQLt'
ORDER BY schema_id, name

declare 
  @id int = 1,
  @cnt int = (select COUNT(*) from #tables),
  @tname sysname,
  @sql nvarchar(4000)

while @id <= @cnt
begin
  select @tname = tname from #tables where id = @id

  begin try 
    exec sspm.CopyTableDefinition
      @SourceTable = @tname,
      @TargetTable = 'tmp_table',
      @UsePS = 0,
      @TargetPSName = null,
      @SkipIndexes = 0,
      @DropTargetIfExists = 1,
      @debug = 0

    exec tSQLt.AssertEqualsTableSchema 
      @Expected = @tname, 
      @actual = 'dbo.tmp_table',
      @Message = @tname

    -- not that simple with identity and computed columns
    --set @sql = 'insert into tmp_table select top 100 * from ' + @tname
    --exec(@sql)

  end try
  begin catch
    declare @err nvarchar(4000) = error_message()
    exec tSQLt.Fail @tname, ' ', @err
  end catch

  if object_id('dbo.tmp_table') is not null
    DROP TABLE dbo.tmp_table

  set @id += 1
end
go

