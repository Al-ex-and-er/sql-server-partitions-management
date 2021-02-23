--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
/*
Not partitioned tables are supported too.

if @From and/or @To is not specified, function will detect required partitions automatically

  @From is null and EndDateKey is null - all partitions in a table
  @From = X and EndDateKey is null all partitions starting from X, inclusive
  @From = is null and EndDateKey = X all partitions up to X, not inclusive
  @From = X and EndDateKey = Y all partitions in a range [X, Y)

use cases:
1. table-level per-partition maintenance, Action must be REBUILD, non index-specific

exec sspm.Rebuild 
  @TableName = 'Sales.CustomerTransactions', 
  @Action       = 'REBUILD'

exec sspm.Rebuild 
  @TableName = 'Sales.CustomerTransactions', 
  @From = 20180401,
  @To   = 20180402,
  @Action       = 'REBUILD'

2. PK-only 

exec sspm.Rebuild 
  @TableName = 'Sales.CustomerTransactions', 
  @From = 20180401,
  @To   = 20180402,
  @Action       = 'REBUILD',
  @PKOnly       = 1

3. One index only

exec sspm.Rebuild 
  @TableName = 'Sales.CustomerTransactions', 
  @From = 20180401,
  @To   = 20180402,
  @Action       = 'REBUILD',
  @IndexName   = N'IX_Sales_CustomerTransactions'

4. AUTO mode. Action is based on actual fragmentation and Threshold
  if fragmentation is < @SkipIfLess, do nothing
  if fragmentation >= @SkipIfLess an < @RebuildFrom - REORGANIZE
  else REBUILD 
  default for @SkipIfLess is 0
  default for @RebuildFrom is 10

exec sspm.Rebuild 
  @TableName = 'Sales.CustomerTransactions', 
  @Action       = 'AUTO',
  @SkipIfLess  = 1, --percents
  @RebuildFrom = 10 --percents

*/

CREATE OR ALTER PROCEDURE sspm.Rebuild
(
  @TableName sysname,
  @From sql_variant = NULL,
  @To   sql_variant = NULL,
  @IndexName sysname = NULL,
  @Action varchar(10) = 'AUTO',
  @SkipIfLess float = 1.0, --treshold to take any action at all,[0.0, 100.0]
  @RebuildFrom float = 10.0, --treshold for avg_fragmentation_in_percent to decide what to do, REBUILD or REORGANIZE
  @Debug bit = 0,
  @PKOnly bit = 0,
  @Online bit = 1,
  @Online_MAX_DURATION int = NULL,
  @Online_ABORT_AFTER_WAIT varchar(10) = NULL
)
as 
set nocount on

--check @TableName
declare
  @dbName nvarchar(128) = db_name(),
  @tmpDB nvarchar(128), @tmpSchema sysname, @tmpTable sysname, @tmpSrv sysname

select
  @tmpTable  = parsename(@TableName, 1),
  @tmpSchema = isnull(parsename(@TableName, 2), N'dbo'),
  @tmpDB     = isnull(parsename(@TableName, 3), @dbName),
  @tmpSrv    = parsename(@TableName, 4)

if @tmpSrv is not null or @tmpDB <> @dbName
begin
  raiserror('@TableName is not valid', 16, 1)
  return
end

set @TableName = quotename(@tmpSchema) + '.' + quotename(@tmpTable)

if isnull(@SkipIfLess, -1) < 0.0 or @SkipIfLess > 100.0
begin
  raiserror ('@SkipIfLess can be in a range [0.0, 100.0]!', 16, 0)
  return
end

if isnull(@RebuildFrom, -1) < 0.0 or @RebuildFrom > 100.0
begin
  raiserror ('@RebuildFrom can be in a range [0.0, 100.0]!', 16, 0)
  return
end

if @RebuildFrom <= @SkipIfLess
begin
  raiserror ('@RebuildFrom should be > @SkipIfLess!', 16, 0)
  return
end

if not @Action in ('REBUILD', 'REORGANIZE', 'AUTO')
begin
  raiserror ('@Action can be REBUILD, REORGANIZE or AUTO!', 16, 0)
  return
end

if  (@Online_MAX_DURATION is not null and @Online_ABORT_AFTER_WAIT is null)
 or (@Online_MAX_DURATION is null and @Online_ABORT_AFTER_WAIT is not null)
begin
  raiserror ('Both @Online_MAX_DURATION and @Online_ABORT_AFTER_WAIT should be specified!', 16, 0)
  return
end

if @Online_ABORT_AFTER_WAIT is not null and @Online_ABORT_AFTER_WAIT not in ('NONE', 'SELF', 'BLOCKERS')
begin
  raiserror ('Wrong @Online_ABORT_AFTER_WAIT!', 16, 0)
  return
end

declare @object_id int = object_id(@TableName)

if @object_id is null
begin
  raiserror ('Can''t find table, @object_id is null!', 16, 0)
  return
end

if @From is not null 
  and @To is not null
  and @From > @To
begin
  raiserror ('@From > @To!', 16, 0)
  return
end

declare 
  @PF sysname, 
  @PFType sysname, --type of partition function
  @partitioned bit = 1

--Get the partition function for the table
SELECT @PF = pf.name
FROM sys.indexes i with(nolock)
  JOIN sys.partition_schemes ps with(nolock) 
    on ps.data_space_id = i.data_space_id
  JOIN sys.partition_functions pf with(nolock)
    on pf.function_id = ps.function_id
WHERE i.object_id = @object_id
  AND i.type in(0, 1)

set @partitioned = iif(@PF is null, 0, 1)

if @Debug > 0
  print '@PF = ' + isnull(@PF, 'NULL')

if @partitioned = 1
begin
  SELECT 
    @PFType = 
      case 
        when t.name in (N'time', N'date', N'smalldatetime', N'datetime', N'bigint', N'int', N'smallint', N'tinyint') 
          then t.name
        when t.name in (N'datetime2', N'datetimeoffset') 
          then t.name + N'(' + cast(p.scale as nvarchar(5)) + N')'
      end   
  FROM sys.partition_functions f with(nolock)
    join sys.partition_parameters p with(nolock)
      on p.function_id = f.function_id
    join sys.types t with(nolock)
      on p.system_type_id = t.system_type_id
  WHERE f.name = @PF

  if @Debug > 0
    print '@PFType = ' + isnull(@PFType, 'NULL')
end

if @From is not null and @To is not null and @partitioned = 0
begin
  raiserror ('Can''t use range on non-partitioned object, @From and @To must be NULL!', 16, 0)
  return
end

declare 
  @MinPartNum int,
  @MaxPartNum int

if @partitioned = 1
begin
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
    FROM sys.partition_functions pf
      outer apply
      ( SELECT top 1 v1.boundary_id, v1.value   
        FROM sys.partition_range_values v1
        WHERE v1.function_id = pf.function_id
          and v1.value <= @From
        ORDER BY v1.boundary_id desc  
      ) b1
      outer apply (SELECT pid = isnull(b1.boundary_id, 0) + 1) v1
      outer apply
      ( SELECT top 1 v2.boundary_id, v2.value   
        FROM sys.partition_range_values v2
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
    WHERE object_id = @object_id
      and index_id <= 1
      and rows > 0

  if @To is null
    SELECT @MaxPartNum = max(partition_number)
    FROM sys.partitions with(nolock)
    WHERE object_id = @object_id
      and index_id <= 1
      and rows > 0
end
else --@partitioned = 0
begin
  select @MinPartNum = 1, @MaxPartNum = 1
end

if @Debug > 0
  print '@MinPartNum = ' + isnull(cast(@MinPartNum as varchar(10)), 'NULL') + ', @MaxPartNum = ' + isnull(cast(@MaxPartNum as varchar(10)), 'NULL')

declare @table_level_rebuild bit = 0 --partitions in all indexes, not a specific index

/*
it should be handled by a new parameter, like @force_table_level
if @PKOnly = 0 and @IndexName is null and @Action = 'REBUILD'
begin
  set @table_level_rebuild = 1
  set @PKOnly = 1
end
*/
--list of partitioned indexes
if object_id('tempdb..#indexes') is not null
  DROP TABLE #indexes

CREATE TABLE #indexes
(
  index_id int not null,
  index_name sysname null --heaps have index without name
)

INSERT INTO #indexes(index_id, index_name)
SELECT i.index_id, i.name
FROM sys.indexes i with(nolock)
  join sys.data_spaces ds with(nolock)
    on ds.data_space_id = i.data_space_id
WHERE object_id = @object_id
  and ds.type = case when @partitioned = 1 then 'PS' else 'FG' end
  and (@PKOnly = 0
        or (@PKOnly = 1 and i.is_primary_key = 1)
      )
  and (@IndexName is null 
        or (@IndexName is not null and i.name = @IndexName)
      )
  and (@table_level_rebuild = 0 
        or (@table_level_rebuild = 1 and i.index_id in (0, 1))
      )

if not exists (select 1 from #indexes)
begin
  raiserror ('No indexes can be found applying parameters', 16, 0)
  return
end

if exists (select 1 from #indexes where index_name is null) --heaps
begin
  set @table_level_rebuild = 1
end

if @Debug > 0
  print '@table_level_rebuild = ' + isnull(cast(@table_level_rebuild as varchar(10)), 'NULL')

if object_id('tempdb..#index_partitions') is not null
  DROP TABLE #index_partitions

CREATE TABLE #index_partitions
(
  index_id int not null,
  index_name sysname null,
  pid int not null,
  action varchar(10) null
)

declare @rc int

INSERT INTO #index_partitions (index_id, index_name, pid, action)
SELECT i.index_id, i.index_name, p.partition_number pid, @Action
FROM #indexes i
  join sys.partitions p with(nolock)
    on p.object_id = @object_id
      and p.index_id = i.index_id
WHERE p.partition_number between @MinPartNum and @MaxPartNum
  and p.rows > 0

set @rc = @@rowcount

if @rc = 0
begin
  if @Debug > 0
    print 'No data, nothing to rebuild!'
  return
end

select @MinPartNum = min(pid), @MaxPartNum = max(pid)
from #index_partitions

declare @log table
(
  index_id int not null,
  pid int not null,
  action varchar(10) null,
  query nvarchar(max)
)

declare
  @cur_pid int,
  @cur_index_id int,
  @cur_index_name sysname,
  @cur_action varchar(10),
  @cmd nvarchar(max),
  @avg_fragmentation_in_percent float,
  @msg varchar(1024),
  @db_id int = db_id()

declare index_partition cursor for
  SELECT pid, index_id, index_name, action
  FROM #index_partitions
  ORDER BY pid, index_id

open index_partition

fetch next from index_partition into @cur_pid, @cur_index_id, @cur_index_name, @cur_action

while @@fetch_status = 0
begin

  if @Action = 'AUTO' --measure fragmentation
  begin
    SELECT @avg_fragmentation_in_percent = avg_fragmentation_in_percent
    FROM sys.dm_db_index_physical_stats (@db_id, @object_id, @cur_index_id, @cur_pid, 'limited')

    set @cur_action = 
      case 
        when @avg_fragmentation_in_percent < @SkipIfLess
          then 'SKIP'
        when @avg_fragmentation_in_percent < @RebuildFrom
          then 'REORGANIZE' 
        else 'REBUILD' 
      end

      --if @Debug > 0
      begin
        set @msg = '@pid = [' + cast(@cur_pid as varchar(16)) +'/' + cast(@MaxPartNum as varchar(16)) + ']'
          + 'index ' + cast(@cur_index_id as varchar(10))
          + ' fragmentation ' + str(@avg_fragmentation_in_percent, 6, 2)
          + ' action ' + @cur_action

        raiserror(@msg, 0, 0) with nowait
      end
  end

  if @cur_action = 'SKIP'
    goto fetch_next_row

  if @table_level_rebuild = 1 and @partitioned = 1
    set @cmd = 'ALTER TABLE ' + @TableName + ' ' + @cur_action + ' PARTITION = ' + cast(@cur_pid as varchar(10))
  else if @table_level_rebuild = 0 and @partitioned = 1
    set @cmd = 'ALTER INDEX ' + quotename(@cur_index_name) + ' ON ' + @TableName + ' ' + @cur_action + ' PARTITION = ' + cast(@cur_pid as varchar(10))
  else if @table_level_rebuild = 1 and @partitioned = 0
    set @cmd = 'ALTER TABLE ' + @TableName + ' ' + @Action 
  else if @table_level_rebuild = 0 and @partitioned = 0
    set @cmd = 'ALTER INDEX ' + quotename(@cur_index_name) + ' ON ' + @TableName + ' ' + @cur_action

  if @Online = 1 and @cur_action = 'REBUILD'
    if @Online_MAX_DURATION is not null
      set @cmd = @cmd + ' WITH (ONLINE = ON (WAIT_AT_LOW_PRIORITY (MAX_DURATION = ' 
        + cast(@Online_MAX_DURATION as varchar(10)) + ', ABORT_AFTER_WAIT = ' +
        + @Online_ABORT_AFTER_WAIT 
        + ')))'
    else
      set @cmd = @cmd + ' WITH (ONLINE = ON)'

  --Set the command to update the statistics;
  if @Debug = 1
  begin
    print @cmd
    INSERT INTO @log(index_id, pid, action, query)
    VALUES (@cur_index_id, @cur_pid, @cur_action, @cmd)
  end
  else
    exec sp_executesql @cmd

  fetch_next_row:
  fetch next from index_partition into @cur_pid, @cur_index_id, @cur_index_name, @cur_action
end 

close index_partition
deallocate index_partition

if @Debug = 1
begin
  select * from @log
end
go
