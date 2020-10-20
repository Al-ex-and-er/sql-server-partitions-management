/*
  This SP can be used to
    1. switch the range of partitions from @SourceTable to @TargetTable (appropriate partitions in @TargetTable will be emptied)
    2. remove data from the range of partitions in @SourceTable

    @SkipCount - counting amount of rows it the most time-consuming operation. Set @SkipCount = 1 to skip it. SP won't return accurate
              @RowsInserted/@RowsDeleted

  Usage:
  1.
  declare @RowsInserted bigint, @RowsDeleted bigint

  exec sspm.Switch
    @SourceTable = 'dbo.Table1',
    @TargetTable = 'dbo.tmpTable1',
    @From = 20170226,
    @To = 20170227,
    @SkipCount = 0,
    @RowsInserted = @RowsInserted out,
    @RowsDeleted = @RowsDeleted out,
    @Debug = 1
    

  select @RowsInserted, @RowsDeleted

  2.
  declare @RowsInserted bigint, @RowsDeleted bigint

  exec sspm.Switch
    @SourceTable = 'dbo.Table1',
    @TargetTable = NULL,
    @From = 20170226,
    @To = 20170227,
    @SkipCount = 0,
    @RowsInserted = @RowsInserted out,
    @RowsDeleted = @RowsDeleted out,
    @Debug = 2
*/

CREATE OR ALTER PROCEDURE sspm.Switch
(
  @SourceTable sysname,
  @TargetTable sysname,
  @From sql_variant,
  @To   sql_variant,
  @SkipCount tinyint = 0,
  @RowsInserted bigint = 0 out,
  @RowsDeleted  bigint = 0 out,
  @Debug tinyint = 0
)
as
set nocount on

select @RowsDeleted = 0, @RowsInserted = 0

if @SourceTable is null
begin
  raiserror('The source table is not specified!', 16, 1)
  return
end

declare
  @SrcTempTable sysname,
  @TgtTempTable sysname

begin --names parsing
  declare
    @dbName nvarchar(128) = db_name(),
    @tmpObject sysname = @SourceTable,
    @tmpDB nvarchar(128), @tmpSchema sysname, @tmpTable sysname, @tmpSrv sysname

  select
    @tmpTable  = parsename(@tmpObject, 1),
    @tmpSchema = isnull(parsename(@tmpObject, 2), N'dbo'),
    @tmpDB     = isnull(parsename(@tmpObject, 3), @dbName),
    @tmpSrv    = parsename(@tmpObject, 4)

  if @tmpSrv is not null or @tmpDB <> @dbName
  begin
    raiserror('@SourceTable is not valid', 16, 1)
    return
  end

  select
    @SourceTable = quotename(@tmpSchema) + '.' + quotename(@tmpTable),
    @SrcTempTable = '[sspm].'+ quotename(@tmpSchema + '_' + @tmpTable)

  if @TargetTable is not null
  begin
    set @tmpObject = @TargetTable

    select
      @tmpTable  = parsename(@tmpObject, 1),
      @tmpSchema = isnull(parsename(@tmpObject, 2), N'dbo'),
      @tmpDB     = isnull(parsename(@tmpObject, 3), @dbName),
      @tmpSrv    = parsename(@tmpObject, 4)

    if @tmpSrv is not null or @tmpDB <> @dbName
    begin
      raiserror('@TargetTable is not valid', 16, 1)
      return
    end

    select
      @TargetTable = quotename(@tmpSchema) + '.' + quotename(@tmpTable),
      @TgtTempTable = '[sspm].'+ quotename(@tmpSchema + '_' + @tmpTable)
  end
  else
  begin
    set @TgtTempTable = @SrcTempTable
  end
end--/names parsing

declare @SrcObjectID int = object_id(@SourceTable, 'U')

if @SrcObjectID is null
begin
  raiserror('Source table doesn''t exist, @SourceTable = %s', 16, 1, @SourceTable)
  return
end

if @TargetTable is not null and object_id(@TargetTable, 'U') is null
begin
  raiserror('Target table doesn''t exist, @TargetTable = %s', 16, 1, @TargetTable)
  return
end

declare @range_basetype sysname

if @To is not null and @From is not null
begin
  set @range_basetype = cast(sql_variant_property(@From, 'BaseType') as sysname)
  if @range_basetype <> cast(sql_variant_property(@To, 'BaseType') as sysname)
  begin
    raiserror('Values should be of the same type!', 16, 1)
    return
  end
  else if @To < @From
  begin
    raiserror('@From should be < @To!', 16, 1)
    return
  end
end

declare
  @PF sysname,
  @PFDecl sysname,
  @function_id int

-- Get the partition function and key
SELECT top 1
  @function_id = pf.function_id,
  @PF = pf.name,
  @PFDecl = pf.name + '(' + quotename(c.name) + ')'
FROM sys.indexes i with (nolock)
  join sys.index_columns ic with(nolock)
    on ic.index_id = i.index_id
      and ic.object_id = i.object_id
  join sys.columns c with(nolock)
    on c.object_id = ic.object_id
      and c.column_id = ic.column_id
  join sys.partition_schemes ps with(nolock)
    on i.data_space_id = ps.data_space_id
  join sys.partition_functions pf with(nolock)
    on pf.function_id = ps.function_id
WHERE i.object_id = @SrcObjectID
  and i.index_id <= 1 --CI or heap
  and ic.partition_ordinal > 0

if @PF is null
begin
  raiserror('The source table is not partitioned!', 16, 1)
  return
end

if @Debug > 0
begin
  raiserror(N'Switching from %s to %s, function %s', 0, 0, @SourceTable, @TargetTable, @PFDecl) with nowait
end

declare
  @MinPartNum int,
  @MaxPartNum int;

--sspm.GetPIDRange
with ranges as
(
SELECT
    pid1 =
      case
        when @From is null then 1
        when v1.pid = pf.fanout and @From != b1.value then null
        when @From != b1.value or b1.value is null then v1.pid + 1
        when @From = b1.value and pf.boundary_value_on_right = 1 then v1.pid
        else v1.pid
      end,
    pid2 =
      case
        when @To is null then pf.fanout
        when v2.pid = 1 then null
        else v2.pid - 1
      end
  FROM sys.partition_functions pf with(nolock)
    outer apply
    ( SELECT top 1 v1.boundary_id, v1.value
      FROM sys.partition_range_values v1 with(nolock)
      WHERE v1.function_id = pf.function_id
        and v1.value <= @From
      ORDER BY v1.boundary_id desc
    ) b1
    outer apply (SELECT pid = isnull(b1.boundary_id, 0) + 1) v1
    outer apply
    ( SELECT top 1 v2.boundary_id, v2.value
      FROM sys.partition_range_values v2 with(nolock)
      WHERE v2.function_id = pf.function_id
        and v2.value <= @To
      ORDER BY v2.boundary_id desc
    ) b2
    outer apply (SELECT pid = isnull(b2.boundary_id, 0) + 1) v2
  WHERE pf.name = @PF
)
SELECT @MinPartNum = pid1, @MaxPartNum = pid2
FROM ranges
WHERE isnull(pid1, 999999) <= isnull(pid2, -1)

if @From is null
  SELECT @MinPartNum = min(partition_number)
  FROM sys.partitions with(nolock)
  WHERE object_id = @SrcObjectID
    and index_id <= 1
    and rows > 0

if @To is null
  SELECT @MaxPartNum = max(partition_number)
  FROM sys.partitions with(nolock)
  WHERE object_id = @SrcObjectID
    and index_id <= 1
    and rows > 0

if @Debug > 0
begin
  raiserror('Switching partitions [%i, %i]', 0, 0, @MinPartNum, @MaxPartNum) with nowait
end

if @MinPartNum is null or @MaxPartNum is null
begin
  raiserror('The range of values should define at least one whole partition to switch!', 0, 0)
  return
end

declare @function_basetype sysname

select @function_basetype = cast(sql_variant_property(r.value, 'BaseType') as sysname)
from sys.partition_range_values r with(nolock)
where r.function_id = @function_id
  and r.boundary_id = 1

if not
 (
    (@range_basetype = @function_basetype)
    or (charindex(N'int', @range_basetype) > 0 and charindex(N'int', @function_basetype) > 0)
 )
begin
  raiserror(N'Input values type (%s) should be of the same type as partition function argument (%s)', 16, 1, @range_basetype, @function_basetype)
  return
end

declare
  @CleanupOnly bit = case when @TargetTable is null then 1 else 0 end,
  @Cmd nvarchar(max),
  @msg nvarchar(2048)

begin
  --@RowsDeleted
  --count rows in the destination table, if we are moving partitions between sorce and destination
  --count rows in the source table if we are moving partitions between source and none
  set @Cmd =
    case
      when @CleanupOnly = 0 and @SkipCount = 0 then N'SELECT @Rows = count_big(*) FROM ' + @TargetTable + N' with(nolock) WHERE $partition.' + @PFDecl + N' BETWEEN @min and @max'
      when @CleanupOnly = 1 and @SkipCount = 0 then N'SELECT @Rows = count_big(*) FROM ' + @SourceTable + N' with(nolock) WHERE $partition.' + @PFDecl + N' BETWEEN @min and @max'
      when @CleanupOnly = 0 and @SkipCount = 1 then N'SELECT top 1 @Rows = 1 FROM ' + @TargetTable + N' with(nolock) WHERE $partition.' + @PFDecl + N' BETWEEN @min and @max'
      when @CleanupOnly = 1 and @SkipCount = 1 then N'SELECT top 1 @Rows = 1 FROM ' + @SourceTable + N' with(nolock) WHERE $partition.' + @PFDecl + N' BETWEEN @min and @max'
    end

  if @Debug > 1
  begin
    set @msg = replace(replace(@Cmd, N'@Min', cast(@MinPartNum as nvarchar(6))), N'@Max', cast(@MaxPartNum as nvarchar(6)))
    raiserror(@msg, 0, 0) with nowait
  end

  set @RowsDeleted = 0
  exec sp_executesql @Cmd, N'@Rows bigint output, @min int, @max int', @Rows = @RowsDeleted output, @min = @MinPartNum, @max = @MaxPartNum
  set @RowsDeleted = isnull(@RowsDeleted, 0)

  if @Debug > 1
  begin
    raiserror('@RowsDeleted = %I64d', 0, 0, @RowsDeleted) with nowait
  end
end

if @CleanupOnly = 1 and @RowsDeleted = 0 --Nothing to delete, the the source table is empty
begin
  if @Debug > 0
  begin
    raiserror('Nothing to delete, the source table is empty', 0, 0) with nowait
  end

  return
end


declare
  @IsDestEmpty bit, @IsSrcEmpty bit,
  @HasNonEmptyDst bit = 0,
  @HasNonEmptySrc bit = 0,
  @CurPartNum int = @MinPartNum,
  @RetryCount tinyint

declare
  @CmdSwitchDestAndTruncate nvarchar(2048) = 'ALTER TABLE ' + @TargetTable + ' SWITCH PARTITION @PartNum TO ' + @TgtTempTable + ' PARTITION @PartNum; TRUNCATE TABLE ' + @TgtTempTable,
  @CmdSwitchSrcAndTruncate  nvarchar(2048) = 'ALTER TABLE ' + @SourceTable + ' SWITCH PARTITION @PartNum TO ' + @SrcTempTable + ' PARTITION @PartNum; TRUNCATE TABLE ' + @SrcTempTable,
  @CmdSwitchSourceToDest    nvarchar(2048) = 'ALTER TABLE ' + @SourceTable + ' SWITCH PARTITION @PartNum TO ' + @TargetTable  + ' PARTITION @PartNum ',
  @CmdCheckIfDestPartEmpty  nvarchar(2048) = 'SELECT top 1 @Rows = 1 FROM ' + @TargetTable + ' with(nolock) WHERE $partition.' + @PFDecl + ' = @PartNum',
  @CmdCheckIfSrcPartEmpty   nvarchar(2048) = 'SELECT top 1 @Rows = 1 FROM ' + @SourceTable + ' with(nolock) WHERE $partition.' + @PFDecl + ' = @PartNum'

if   @CmdSwitchDestAndTruncate is null
  or @CmdSwitchSrcAndTruncate is null
  or @CmdSwitchSourceToDest is null
  or @CmdCheckIfDestPartEmpty is null
  or @CmdCheckIfSrcPartEmpty is null
begin
  raiserror('Command prepared to exec is null', 16, 1)
  return
end

if @RowsDeleted > 0
begin
  declare @Table sysname, @TempTable sysname

  if @CleanupOnly = 0
  begin
    if @Debug > 0 print N'The target table has data, let''s check the destination temp table, @TgtTempTable = ' + @TgtTempTable
    set @Table = @TargetTable
    set @TempTable = @TgtTempTable
  end
  else
  begin
    if @Debug > 0 print N'The source table has data, let''s check the destination table, @SrcTempTable = ' + @SrcTempTable
    set @Table = @SourceTable
    set @TempTable = @SrcTempTable
  end

  if object_id(@TempTable) is null
  begin --if no such table, create it
    exec sspm.CopyTableDefinition @SourceTable = @Table, @TargetTable = @TempTable, @UsePS = 1
  end
  else
  begin -- if table exists, clean it up
    set @Cmd = N'TRUNCATE TABLE ' + @TempTable
    if @Debug > 0 print @Cmd
    exec sp_executesql @Cmd
  end
end

while @CurPartNum <= @MaxPartNum
begin

  if @CleanupOnly = 0 --check destination table
  begin
    -- check if partitions in destination table empty or not
    --if destination is not empty, we have to clean it up
    if @Debug > 1
    begin
      set @msg = replace(@CmdCheckIfDestPartEmpty, N'@PartNum', cast(@CurPartNum as nvarchar(6)))
      raiserror(@msg, 0, 0) with nowait
    end

    set @IsDestEmpty = null
    exec sp_executesql @CmdCheckIfDestPartEmpty, N'@Rows int output, @PartNum int', @Rows = @IsDestEmpty output, @PartNum = @CurPartNum
    set @IsDestEmpty = case when @IsDestEmpty is null then 1 else 0 end

    if @Debug > 1 print '@IsDestEmpty = ' + cast(@IsDestEmpty as char(1))

    if @IsDestEmpty = 0
    begin
      if @HasNonEmptyDst = 0
      begin
        set @HasNonEmptyDst = 1
        if @Debug > 0 print 'Found not-empty partition in destination table'
      end

      -- Switch out and truncate destination partition before switch in
      if @Debug > 0
      begin
        set @msg = replace(@CmdSwitchDestAndTruncate, N'@PartNum', cast(@CurPartNum as nvarchar(6)))
        raiserror(@msg, 0, 0) with nowait
      end

      set @RetryCount = 0
      retry1:
      begin try
        exec sp_executesql @CmdSwitchDestAndTruncate, N'@PartNum int', @PartNum = @CurPartNum
      end try
      begin catch
        if @Debug > 0 print 'failed to switch partition to @TgtTempTable'
        if @RetryCount = 0
        begin
          --let's try to re-create target table and re-try
          exec sspm.CopyTableDefinition @SourceTable = @TargetTable, @TargetTable = @TgtTempTable, @UsePS = 1
          set @RetryCount = 1
          goto retry1
        end
        else throw; --fail
      end catch
    end --@IsDestEmpty = 0
  end --@CleanupOnly = 0

  --check if source partition empty
  if @Debug > 1
  begin
    set @msg = replace(@CmdCheckIfSrcPartEmpty, N'@PartNum', cast(@CurPartNum as nvarchar(6)))
    raiserror(@msg, 0, 0) with nowait
  end

  set @IsSrcEmpty = null
  exec sp_executesql @CmdCheckIfSrcPartEmpty, N'@Rows int output, @PartNum int', @Rows = @IsSrcEmpty output, @PartNum = @CurPartNum
  set @IsSrcEmpty = case when @IsSrcEmpty is null then 1 else 0 end

  if @Debug > 1 print '@IsSrcEmpty = ' + cast(@IsSrcEmpty as char(1))

  if @IsSrcEmpty = 0
  begin
    if @HasNonEmptySrc = 0
    begin
      set @HasNonEmptySrc = 1
      if @Debug > 0 print 'Found not-empty partition in the source table'
    end

    if @CleanupOnly = 0
    begin
      -- switch partition from source to target partition
      if @Debug > 0
      begin
        set @msg = replace(@CmdSwitchSourceToDest, N'@PartNum', cast(@CurPartNum as nvarchar(6)))
        raiserror(@msg, 0, 0) with nowait
      end

      exec sp_executesql @CmdSwitchSourceToDest, N'@PartNum int', @PartNum = @CurPartNum
    end --@CleanupOnly = 0
    else
    begin
      -- switch partition from source to target table
      if @Debug > 0
      begin
        set @msg = replace(@CmdSwitchSrcAndTruncate, N'@PartNum', cast(@CurPartNum as nvarchar(6)))
        raiserror(@msg, 0, 0) with nowait
      end

      set @RetryCount = 0
    retry2:
      begin try
        exec sp_executesql @CmdSwitchSrcAndTruncate, N'@PartNum int', @PartNum = @CurPartNum
      end try
      begin catch
        if @Debug > 0 print 'failed to switch partition to @SrcTempTable'
        if @RetryCount = 0
        begin
          --let's try to re-create target table and re-try
          exec sspm.CopyTableDefinition @SourceTable = @SourceTable, @TargetTable = @SrcTempTable, @UsePS = 1
          set @RetryCount = 1
          goto retry2
        end
        else throw; --fail
      end catch

    end --@CleanupOnly = 1
  end --@IsSrcEmpty = 0

  set @CurPartNum += 1
end --/while

if @CleanupOnly = 0 and @HasNonEmptySrc = 1
begin
  --count inserted rows in destination table
  if @SkipCount = 0
    set @Cmd = 'SELECT @Rows = count_big(*) FROM ' + @TargetTable + ' with(nolock) WHERE $partition.' + @PFDecl + ' BETWEEN @min and @max'
  else
    set @Cmd = 'SELECT top 1 @Rows = 1 FROM ' + @TargetTable + ' with(nolock) WHERE $partition.' + @PFDecl + ' BETWEEN @min and @max'

  if @Debug > 1
  begin
    set @msg = replace(replace(@Cmd, N'@min', cast(@MinPartNum as varchar(12))), N'@max', cast(@MaxPartNum as nvarchar(6)))
    raiserror(@msg, 0, 0) with nowait
  end

  set @RowsInserted = 0
  exec sp_executesql @Cmd, N'@Rows bigint output, @min int, @max int', @Rows = @RowsInserted output, @min = @MinPartNum, @max = @MaxPartNum
  set @RowsInserted = isnull(@RowsInserted, 0)

  if @Debug > 1
  begin
    raiserror('count(*) = %I64d', 0, 0, @RowsInserted) with nowait
  end
end
else
begin
  set @RowsInserted = 0

  if @Debug > 0
  begin
    raiserror('Nothing to switch', 0, 0) with nowait
  end
end
/*
if @HasNonEmptyDst = 1
begin
    -- Drop the partition
  set @Cmd = 'TRUNCATE TABLE ' + @TgtTempTable

  if @Debug > 0
    raiserror(@Cmd, 0, 0) with nowait

  exec sp_executesql @Cmd
end
*/

go
