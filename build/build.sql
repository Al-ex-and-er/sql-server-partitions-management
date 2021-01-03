if not exists (select 1 from sys.schemas where name = 'sspm')
begin
  EXEC ('CREATE SCHEMA sspm')
end
go
/*
  Returns compression of non-partitioned row table
*/
CREATE OR ALTER FUNCTION sspm.GetCompression (@TableName sysname)
returns nvarchar(60)
with returns null on null input
as
begin
return
(
  SELECT top 1 p.data_compression_desc
  FROM sys.indexes i with(nolock)
    join sys.partitions p with(nolock)
      on p.[object_id] = i.[object_id]
        and p.index_id = i.index_id
  WHERE i.index_id in (0, 1)
    and p.partition_number = 1
    and i.[object_id] = object_id(@TableName)
)
end
go
/*
  Returns FileGroup of non-partitioned row table
*/
CREATE OR ALTER FUNCTION sspm.GetFG (@TableName sysname)
returns sysname
with returns null on null input
as
begin
return
(
  SELECT top 1 fg.name
  FROM sys.indexes i with(nolock)
    inner join sys.filegroups fg with(nolock)
      on i.data_space_id = fg.data_space_id
   WHERE i.[object_id] = object_id(@TableName)
     and i.index_id in (0, 1)
)
end
go
/*
  Returns partitioning function of a partitioned table
*/
CREATE OR ALTER FUNCTION sspm.GetPF (@TableName sysname)
returns sysname
with returns null on null input
as
begin
return
(
  SELECT top 1 pf.name
  FROM sys.indexes i with(nolock)
    join sys.partition_schemes ps with(nolock) 
      on ps.data_space_id = i.data_space_id
    join sys.partition_functions pf with(nolock) 
      on pf.function_id = ps.function_id
  WHERE  i.object_id = object_id(@TableName)
    AND i.type in(0, 1)
)
end
go
/*
  Returns partition id of @PFName for a @value
  Returns exactly the same result as $partition.PFName(@value) so no any sense to use this function instead
*/
CREATE OR ALTER FUNCTION sspm.GetPID (@PFName sysname, @value sql_variant)
returns int
as
begin
return
(
  SELECT 
    pid =  
      case   
        when v.boundary_id is null then 1  
        when @value = value and pf.boundary_value_on_right = 0 then v.boundary_id  
        else v.boundary_id + 1  
      end  
  FROM sys.partition_functions pf
    outer apply
    ( SELECT top 1 v.boundary_id, v.value   
      FROM sys.partition_range_values v
      WHERE v.function_id = pf.function_id
        and v.value <= @value
      ORDER BY v.boundary_id desc  
    ) v
  WHERE pf.name = @PFName
)
end
go
/*
This function takes partition function and range of points, and returns range of partition IDs.
It is two functions (sspm.GetStartPID, sspm.GetStopPID) combined in one.
Function returns only valid ranges so pid1 <= pid2 or return nothing.
if @value1 >= @value2, result will be NULL
*/
CREATE OR ALTER FUNCTION sspm.GetPIDRange (@PFName sysname, @value1 sql_variant, @value2 sql_variant)
RETURNS table
AS
RETURN
(
  with ranges as
  (
  SELECT 
      pid1 =  
        case 
          when @value1 is null then 1
          when v1.pid = pf.fanout and @value1 != b1.value then null
          when @value1 != b1.value or b1.value is null then v1.pid + 1
          when @value1 = b1.value and pf.boundary_value_on_right = 1 then v1.pid
          else v1.pid
        end,
      pid2 =  
        case 
          when @value2 is null then pf.fanout
          when v2.pid = 1 then null
          else v2.pid - 1
        end  
    FROM sys.partition_functions pf
      outer apply
      ( SELECT top 1 v1.boundary_id, v1.value   
        FROM sys.partition_range_values v1
        WHERE v1.function_id = pf.function_id
          and v1.value <= @value1
        ORDER BY v1.boundary_id desc  
      ) b1
      outer apply (SELECT pid = isnull(b1.boundary_id, 0) + 1) v1
      outer apply
      ( SELECT top 1 v2.boundary_id, v2.value   
        FROM sys.partition_range_values v2
        WHERE v2.function_id = pf.function_id
          and v2.value <= @value2
        ORDER BY v2.boundary_id desc  
      ) b2
      outer apply (SELECT pid = isnull(b2.boundary_id, 0) + 1) v2
    WHERE pf.name = @PFName
  )
  SELECT pid1, pid2
  FROM ranges
  WHERE isnull(pid1, 999999) <= isnull(pid2, -1)
)
go
/*
  Returns partitioning schema of a partitioned table
*/
CREATE OR ALTER FUNCTION sspm.GetPS (@TableName sysname)
returns sysname
with returns null on null input
as
begin
return
(
  SELECT top 1 ps.name
  FROM sys.indexes i with(nolock)
    join sys.partition_schemes ps with(nolock) 
      on ps.data_space_id = i.data_space_id
  WHERE  i.object_id = object_id(@TableName)
    AND i.type in(0, 1)
)
end
go
/*
  When we switch out partition, we can handle only partition as a whole.
  When we provide a range of values to switch out, we have to find out first and last whole partitions inside of this range.
  This function returns ID of the FIRST whole partition that can be switched out, @value begins the range.
  Function answers question, what is the first full partition that includes @value or comes after the value?
  
  Let's consider an example, partition function with two points, 10 and 20.

  PID      1      2      3
  points -----10-----20----->
           ^  ^   ^  ^   ^
  @value   5  10  15 20  25

  If function is RIGHT, it forms these logical ranges

  pid range
  --- -----
   #1 [-inf, 10)
   #2 [10, 20)
   #3 [20, +inf)

  What partition should be first in a range?

  value result explanation
  ----- ------ --------------
   NULL     #1 the only way to access partition #1
      5     #2 this value is in the middle of PID #1. We can address only whole partition so we can start only from PID #2
     10     #2 this value _STARTS_ PID #2 so we can use PID #2 as a first partition
     15     #3 this value is in the middle of PID # 2, we can start only from PID #3
     20     #3 this value starts PID #3, we can use it
     25     -- this value is in the middle of the rightmost partition. There is no next partition so result is NULL

  For the LEFT function, ranges are like this

  pid range
  --- -----
   #1 (-inf, 10]
   #2 (10, 20]
   #3 (20, +inf]

  value result explanation
  ----- ------ --------------
   NULL     #1 the only way to access partition #1
      5     #2 this value is in the middle of PID #1. We can address only whole partition so we can start only from PID #2
     10     #2 this value _ENDS_ PID #1 and the next available partition is PID #2
     15     #3 this value is in the middle of PID # 2, we can start only from PID #3
     20     #3 this value ends PID #2, next available partition is PID #3
     25     -- this value is in the middle of the rightmost partition. There is no next partition so result is NULL

  As we can see, results for LEFT and RIGHT functions are the same

*/
CREATE OR ALTER FUNCTION sspm.GetStartPID (@PFName sysname, @value sql_variant)
returns int
as
begin
return
(
  SELECT 
    pid =  
      case 
        when @value is null then 1 -- null means 'from the leftmost partition'
        when v.pid = pf.fanout and @value != b.value then null --last partition but not the first point
        when @value != b.value or b.value is null then v.pid + 1 --in the middle of partition or first partition
        when @value = b.value and pf.boundary_value_on_right = 1 then v.pid --beginning of partition for RIGHT functions
        else v.pid --other
      end  
  FROM sys.partition_functions pf
    outer apply --find a boundary at @value or to the left from @value
    ( SELECT top 1 v.boundary_id, v.value   
      FROM sys.partition_range_values v
      WHERE v.function_id = pf.function_id
        and v.value <= @value
      ORDER BY v.boundary_id desc  
    ) b
    outer apply (SELECT pid = isnull(b.boundary_id, 0) + 1) v --convert boundary_id to RIGHT partition ID
  WHERE pf.name = @PFName
)
end
go
/*
  When we switch out partition, we can handle only partition as a whole.
  When we provide a range of values to switch out, we have to find out first and last whole partitions inside of this range.
  This function returns ID of the LAST whole partition that can be switched out, @value ends the range.
  Function answers question, what is the last full partition that includes @value or comes before the value?
  
  Let's consider an example, partition function with two points, 10 and 20.

  PID      1      2      3
  points -----10-----20----->
           ^  ^   ^  ^   ^
  @value   5  10  15 20  25

  If function is RIGHT, it forms these logical ranges

  pid range
  --- -----
   #1 [-inf, 10)
   #2 [10, 20)
   #3 [20, +inf)

  What partition should be first in a range?

  value result explanation
  ----- ------ --------------
      5     -- this value is in the middle of PID #1. There is no previous partition so result is NULL
     10     #1 this value starts PID #2 so we can use previous partition, PID #1
     15     #1 this value is in the middle of PID # 2, we can use previous one, PID #1
     20     #2 this value starts PID #3, we can use PID #2 as a last full partition
     25     #2 this value is in the middle of the rightmost partition. Result is a previous partition
   NULL     #3 the only way to access last partition

  For the LEFT function, ranges are like this

  pid range
  --- -----
   #1 (-inf, 10]
   #2 (10, 20]
   #3 (20, +inf]

  value result explanation
  ----- ------ --------------
      5     -- this value is in the middle of PID #1. There is no previous partition so result is NULL
     10     #1 this value ends PID #1 so we can use PID #1 
     15     #1 this value is in the middle of PID # 2. Since it is in the middle, we should use previous one, PID #1
     20     #2 this value ends PID #2, we can use it
     25     #2 this value is in the middle of the rightmost partition. Result is a previous partition
   NULL     #3 the only way to access last partition

  As we can see, results for LEFT and RIGHT functions are the same

*/
CREATE OR ALTER FUNCTION sspm.GetStopPID (@PFName sysname, @value sql_variant)
returns int
as
begin
return
(
  SELECT 
    pid =  
      case 
        when @value is null then pf.fanout -- null means 'to the the rightmost partition'
        when v.pid = 1 then null --We can't use first partition as a last partition
        --if values is between two points, we should take previous one
        --when @value != value then v.pid - 1
        --if value is at the point and function is LEFT, we can stop at this partition
        --when @value = value and pf.boundary_value_on_right = 0 then v.pid - 1
        --LEFT functions use the same numbers as RIGHT ones
        else v.pid - 1
      end  
  FROM sys.partition_functions pf
    outer apply --find a boundary at @value or to the left from @value
    ( SELECT top 1 v.boundary_id, v.value   
      FROM sys.partition_range_values v
      WHERE v.function_id = pf.function_id
        and v.value <= @value
      ORDER BY v.boundary_id desc  
    ) b
    outer apply (select pid = isnull(b.boundary_id, 0) + 1) v --convert boundary_id to RIGHT partition ID
  WHERE pf.name = @PFName
)
end
go
CREATE OR ALTER FUNCTION sspm.GetTablesByFG (@FGName sysname)
RETURNS table
AS
RETURN
(
  SELECT
    i.object_id,
    table_name = sch.name + '.' + o.name,
    is_partitioned = 1
  FROM sys.data_spaces ds with(nolock) --data_space is a FG
    join sys.destination_data_spaces dds with(nolock) --link to partition_scheme_id
      on ds.data_space_id = dds.data_space_id
        and dds.destination_id = 1
    join sys.indexes i with(nolock)
      on i.data_space_id = dds.partition_scheme_id --if index is partitioned, i.data_space_id is a partition_scheme_id
    join sys.objects o with(nolock)
      on o.object_id = i.object_id
    join sys.schemas sch with(nolock)
      on sch.schema_id = o.schema_id
  WHERE ds.name = @FGName
    and ds.type = 'FG'
    and i.index_id in (0, 1)

  UNION ALL

  SELECT
    i.object_id,
    table_name = sch.name + '.' + o.name,
    is_partitioned = 0
  FROM sys.data_spaces ds with(nolock)
    join sys.indexes i with(nolock)
      on  i.data_space_id = ds.data_space_id
    join sys.objects o with(nolock)
      on o.object_id = i.object_id
    join sys.schemas sch with(nolock)
      on sch.schema_id = o.schema_id
  WHERE ds.name = @FGName
    and ds.type = 'FG'
    and i.index_id in (0, 1)
)
go
CREATE OR ALTER FUNCTION sspm.GetTablesByPF (@PFName sysname)
RETURNS table
AS
RETURN
(
  SELECT
    i.object_id,
    table_name = sch.name + '.' + o.name
  FROM sys.partition_functions pf with(nolock)
    join sys.partition_schemes ps with(nolock)
      on ps.function_id = pf.function_id
    join sys.indexes i with(nolock)
      on i.data_space_id = ps.data_space_id
    join sys.objects o with(nolock)
      on o.object_id = i.object_id
    join sys.schemas sch with(nolock)
      on sch.schema_id = o.schema_id
  WHERE i.index_id in (0, 1)
    and pf.name = @PFName
)
go
CREATE OR ALTER FUNCTION sspm.GetTablesByPS (@PSName sysname)
RETURNS table
AS
RETURN
(
  SELECT
    i.object_id,
    table_name = sch.name + '.' + o.name
  FROM sys.partition_schemes ps with(nolock)
    join sys.indexes i with(nolock)
      on i.data_space_id = ps.data_space_id
    join sys.objects o with(nolock)
      on o.object_id = i.object_id
    join sys.schemas sch with(nolock)
      on sch.schema_id = o.schema_id
  WHERE i.index_id in (0, 1)
    and ps.name = @PSName
)
go
go
go
/*
The main goal of sspm.CopyTableDefinition is to create a table that can be a target of switch partition.
Because of this it is not a perfect table schema replication procedure.
Procedure is long and ugly, I'd love to see this task solved as part of SQL Server standard tooling.

  @SourceTable - one-point name, schema.table_name. two-point name is fine if DB name is the same as current DB.
  @TargetTable - same as @SourceTable
  @UsePS bit, if 0 creates @TargetTable as non-partitioned table, if 1 uses PS of source table (or @TargetPSName if specified)
  @SkipIndexes
    0   - create all indexes
    1   - omit all non-clustered indexes
    2   - omit all columnstore clustered indexes
    255 - omit all indexes
  @Debug
    1 - don't do any actions, print-only
    0 - do all actions, don't print

How to move table on a new partition schema
  @UsePS = 1,
  @TargetPSName = 'New PS name',

How to create new table on the same PS
  @UsePS = 1
  @TargetPSName = null --default
*/

CREATE OR ALTER PROCEDURE sspm.CopyTableDefinition
  @SourceTable sysname,
  @TargetTable sysname,
  @UsePS bit = 0,
  @TargetPSName sysname = null,
  @SkipIndexes tinyint = 0,
  @DropTargetIfExists bit = 0,
  @Debug bit = 0
as
set nocount on
set ansi_padding on

if ltrim(rtrim(isnull(@SourceTable, ''))) = ''
begin
  raiserror('@SourceTable can''t be NULL or empty', 16, 1)
  return
end

if ltrim(rtrim(isnull(@TargetTable, ''))) = ''
begin
  raiserror('@TargetTable can''t be NULL or empty', 16, 1)
  return
end

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

set @SourceTable = quotename(@tmpSchema) + '.' + quotename(@tmpTable)
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

set @TargetTable = quotename(@tmpSchema) + '.' + quotename(@tmpTable)

if @Debug = 1
  print 'source table: ' + @SourceTable + ' target table: ' + @TargetTable

if @UsePS = 0 and @TargetPSName is not null
begin
  raiserror('@TargetPSName should not be specified if @UsePS = 0', 16, 1)
  return
end

if @TargetPSName is not null
  and not exists(select 1 from sys.partition_schemes where name = @TargetPSName)
begin
  raiserror('Can''t find @TargetPSName', 16, 1)
  return
end

if @SkipIndexes not in(0, 1, 2, 255)
begin
  raiserror('Not supported value in @SkipIndexes', 16, 1)
  return
end

declare @ObjectId int = object_id(@SourceTable, 'U')

if @ObjectId is null
begin
  raiserror('Source table can''t be found', 16, 1)
  return
end

declare
  @SkipNCIndexes  bit = case when @SkipIndexes = 1   then 1 else 0 end,
  @SkipCCIndexes  bit = case when @SkipIndexes = 2   then 1 else 0 end,
  @SkipAllIndexes bit = case when @SkipIndexes = 255 then 1 else 0 end

declare
  @Compression varchar(20),
  @SourcePSName sysname,
  @PartColumn sysname,
  @FileGroupName sysname

-- Object Info
SELECT
  @Compression = p.data_compression_desc,
  @PartColumn = quotename(col.PartColumn)
FROM sys.indexes i with(nolock)
  join sys.partitions p with(nolock)
    on p.[object_id] = i.[object_id]
      and p.index_id = i.index_id
  outer apply
  (
    select PartColumn = c.name
    from sys.index_columns ic with(nolock)
     join sys.columns c with(nolock)
        on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
    where
      ic.index_id = i.index_id
      and ic.[object_id] = i.[object_id]
      and ic.partition_ordinal > 0
  )col
WHERE
  i.index_id in (0, 1)
  and p.partition_number = 1
  and i.[object_id] = @ObjectId

if @Debug = 1
  print 'data compression: ' + isnull(@Compression, 'NONE') + ' partitioning column: ' + isnull(@PartColumn, '<none>')

SELECT
  @FileGroupName = quotename(fg.name),
  @SourcePSName = quotename(ps.name)
FROM sys.indexes i with(nolock)
  inner join sys.partitions p with(nolock)
    on i.[object_id] = p.[object_id]
      and i.index_id = p.index_id
  left join sys.partition_schemes ps with(nolock)
    on i.data_space_id = ps.data_space_id
  left join sys.destination_data_spaces dds with(nolock)
    on ps.data_space_id = dds.partition_scheme_id
      and p.partition_number = dds.destination_id
  inner join sys.filegroups fg with(nolock)
    on coalesce(dds.data_space_id, i.data_space_id) = fg.data_space_id
 WHERE
  i.[object_id] = @ObjectId

if @Debug = 1
  print 'source file group: ' + isnull(@FileGroupName, '-') + ' source partitioning schema: ' + isnull(@SourcePSName, '-')

if @UsePS = 1 and @SourcePSName is null and @TargetPSName is null
begin
  print '@UsePS = 1 but source table is not partitioned and @TargetPSName is not specified, @UsePS was re-set to 0'
  set @UsePS = 0
end

declare @PSName sysname = @SourcePSName

if @UsePS = 1 and @TargetPSName is not null
  set @PSName = @TargetPSName

if @Debug = 1
  print 'target partitioning schema: ' + @PSName

declare @cmd nvarchar(max)
declare @ddl table(id int identity(1,1) not null, query nvarchar(max) not null)

if object_id(@TargetTable) is not null
begin
  if @DropTargetIfExists = 1
  begin
    set @cmd = N'if (object_id(''' + @TargetTable + ''') is not null) DROP TABLE ' + @TargetTable
    INSERT INTO @ddl(query) VALUES (@cmd)
    if @Debug = 1 print @cmd
  end
  else
  begin
    if @Debug = 0
    begin
      raiserror('@TargetTable exists, please use @DropTargetIfExists = 1 to drop it', 16, 1)
      return
    end
    else
      print '@TargetTable exists, please use @DropTargetIfExists = 1 to drop it'
  end
end

declare @PhysicalLocation varchar(1024) = ''

if @UsePS = 0
begin
  if @FileGroupName is not null
    set @PhysicalLocation = ' ON ' + @FileGroupName
end
else
  set @PhysicalLocation = ' ON ' + @PSName + '(' + @PartColumn + ') '

declare @TableOptions varchar(2048) = ''

if @Compression not in ('NONE', 'COLUMNSTORE', 'COLUMNSTORE_ARCHIVE')
  set @TableOptions = ' WITH (DATA_COMPRESSION = ' + @Compression + ')'

declare
  @createTableHeader nvarchar(1024) = N'CREATE TABLE '+ @TargetTable + N'(',
  @createTableColumns nvarchar(max) = N'',
  @createTableFooter nvarchar(1024) = N')' + @PhysicalLocation + @TableOptions

;with column_def as
(
  SELECT
    column_name = c.name,
    column_ordinal = row_number() over(partition by c.object_id order by c.column_id asc),
    c.is_nullable,
    data_type = st.name,
    max_length =
      case
        when c.max_length = -1 then 'max'
        when st.name in (N'nchar', N'nvarchar', N'ntext') then cast(c.max_length/2 as varchar(16))
        else cast(c.max_length as varchar(16))
      end,
    c.precision,
    c.scale,
    c.collation_name,
    cc.definition,
    cc.is_persisted,
    is_user_type = case when c.system_type_id <> c.user_type_id then 1 else 0 end
  FROM sys.columns c
    join sys.types st
      ON st.user_type_id = c.user_type_id
    left join sys.identity_columns ic
      on c.object_id = ic.object_id AND c.column_id = ic.column_id
    left join sys.computed_columns cc
      on c.object_id = cc.object_id and c.column_id = cc.column_id
  WHERE c.object_id = @ObjectId
)
select @createTableColumns = @createTableColumns +
  N'[' + column_name + N'] '
  + case
      when definition is null and is_user_type = 0
        then --non-calculated column
          data_type
          + case when collation_name is not null and data_type not in (N'text', N'ntext') then N'(' + max_length + N') collate ' + collation_name else N'' end
          + case when data_type in (N'decimal', N'numeric') then N'(' + cast(precision as varchar(16)) + N', ' + cast(scale as varchar(16)) + N')' else N'' end
          + case when data_type in (N'float') then N'(' + cast(precision as varchar(16)) + N')' else N'' end
          + case when data_type in (N'datetime2', N'datetimeoffset') then N'(' + cast(scale as varchar(16)) + N')' else N'' end
          + case when data_type in (N'varbinary', N'binary') then N'(' + max_length + N')' else N'' end
          + case is_nullable when 0 then N' not null' else N' null' end
      when is_user_type = 1
        then data_type
          + case is_nullable when 0 then N' not null' else N' null' end
    else --calculated column
      N' as ' + definition
      + case is_persisted when 1 then N' PERSISTED' else N'' end
    end
  + N', '
from column_def
order by column_ordinal

if len(@createTableColumns) > 1
  set @createTableColumns = substring(@createTableColumns, 1, len(@createTableColumns)-1)

-- Create the table
set @cmd = @createTableHeader + @createTableColumns + @createTableFooter

INSERT INTO @ddl(query) VALUES (@cmd)

if @Debug = 1 print @cmd

if @SkipAllIndexes = 0
begin --Build indexes
  declare
    @IndexId int,
    @IndexName nvarchar(255),
    @IsUnique bit,
    @IsUniqueConstraint bit,
    @IsPrimaryKey bit,
    @IsIncremental bit,
    @FilterDefinition nvarchar(max),
    @IndexType int,
    @count int = 0,
    @Unique nvarchar(255),
    @KeyColumns nvarchar(max),
    @IncludedColumns nvarchar(max),
    @DataCompressionDesc nvarchar(60),
    @Options nvarchar(2048)

  declare indexcursor cursor for
    SELECT
      i.index_id,
      i.name,
      i.is_unique,
      i.is_unique_constraint,
      i.is_primary_key,
      i.filter_definition,
      i.[type],
      p.data_compression_desc,
      quotename(fg.name),
      quotename(ps.name),
      st.is_incremental
    FROM sys.indexes i
      join sys.partitions p with(nolock)
        on p.[object_id] = i.[object_id]
        and p.index_id = i.index_id
      left join sys.partition_schemes ps with(nolock)
        on i.data_space_id = ps.data_space_id
      left join sys.destination_data_spaces dds with(nolock)
        on ps.data_space_id = dds.partition_scheme_id
        and p.partition_number = dds.destination_id
      left join sys.filegroups fg with(nolock)
        on coalesce(dds.data_space_id, i.data_space_id) = fg.data_space_id
      join sys.stats st with(nolock)
        on st.[object_id] = i.[object_id] AND st.name = i.name
    WHERE i.[object_id] = @ObjectId
      and p.partition_number = 1
    ORDER BY i.index_id

  open indexcursor

  fetch next from indexcursor into @IndexId, @IndexName, @IsUnique, @IsUniqueConstraint, @IsPrimaryKey, @FilterDefinition, @IndexType, @DataCompressionDesc, @FileGroupName, @PSName, @IsIncremental

  while @@fetch_status = 0
  begin

    --if @Debug = 1
    --  print
    --      '@IndexId = ' + isnull(cast(@IndexId as varchar(16)), 'NULL') + char(10)
    --    + '@IndexName = '+ isnull(@IndexName, 'NULL') + char(10)
    --    + '@IsUnique = '+ isnull(cast(@IsUnique as char(1)), 'NULL') + char(10)
    --    + '@IsUniqueConstraint = '+ isnull(cast(@IsUniqueConstraint as char(1)), 'NULL') + char(10)
    --    + '@IsPrimaryKey = '+ isnull(cast(@IsPrimaryKey as char(1)), 'NULL') + char(10)
    --    + '@FilterDefinition = '+ isnull(@FilterDefinition, 'NULL') + char(10)
    --    + '@IndexType = '+ isnull(cast(@IndexType as varchar(10)), 'NULL') + char(10)
    --    + '@DataCompressionDesc = '+ isnull(@DataCompressionDesc, 'NULL') + char(10)
    --    + '@FileGroupName = '+ isnull(@FileGroupName, 'NULL') + char(10)
    --    + '@PSName = '+ isnull(@PSName, 'NULL') + char(10)

    set @count += 1

    /*
    if @IndexId = 0 --heap
    begin
      if @UsePS = 0 --move table to target file group
      begin
        set @cmd = N'ALTER TABLE ' + @TargetTable + N' ADD CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_') +  N' UNIQUE CLUSTERED(' + @PartColumn + N')' + @PhysicalLocation
        if @Debug = 1 print @cmd
        exec (@cmd)

        set @cmd = N'ALTER TABLE ' + @TargetTable + N' DROP CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_')
        if @Debug = 1 print @cmd
        exec (@cmd)
      end
      else if @UsePS = 1
      begin --it is only way to put heap on PS
        set @cmd = N'ALTER TABLE ' + @TargetTable + N' ADD CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_') +  N' UNIQUE CLUSTERED(' + @PartColumn + N')'
        if @Debug = 1 print @cmd
        exec (@cmd)

        set @cmd = N'ALTER TABLE ' + @TargetTable + N' DROP CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_') +  N' WITH(MOVE TO ' + @PSName + N'(' + @PartColumn + N'))'
        if @Debug = 1 print @cmd
        exec (@cmd)
      end

      if @DataCompressionDesc != 'NONE'
      begin
        set @cmd = N'ALTER TABLE ' + @TargetTable + N' REBUILD WITH (DATA_COMPRESSION = ' + @DataCompressionDesc + N')'
        if @Debug = 1 print @cmd
        exec (@cmd)
      end

    end --@IndexId = 0 --heap
    else
    */

    if @IndexType = 5 and @SkipCCIndexes = 0 --Columnstore Clustered
    begin
      set @cmd = N'CREATE CLUSTERED COLUMNSTORE INDEX [' + @IndexName + N'] ON ' + @TargetTable

      INSERT INTO @ddl(query) VALUES (@cmd)

      if @Debug = 1 print @cmd
    end
    else if @IndexType = 6 --@SkipNCCIndexes = 0
    begin
      set @IncludedColumns = N''

      SELECT @IncludedColumns = @IncludedColumns + N'[' + c.name + N'], '
      FROM sys.index_columns ic
        join sys.columns c
          on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
      WHERE index_id = @IndexId
        and ic.[object_id] = @ObjectId
        and key_ordinal = 0
      ORDER BY index_column_id

      if len(@IncludedColumns) > 0
        set @IncludedColumns = left(@IncludedColumns, len(@IncludedColumns) - 1)

      if @FilterDefinition is null
        set @FilterDefinition = N''
      else
        set @FilterDefinition = N'WHERE ' + @FilterDefinition + N' '

      set @Options = N''

      if @DataCompressionDesc != N'NONE'
        set @Options = N'DATA_COMPRESSION = ' + @DataCompressionDesc

      if @Options > N''
        set @Options = N' WITH (' + @Options + N')'

        set @cmd = N'CREATE NONCLUSTERED COLUMNSTORE INDEX [' + @IndexName + N'] ON ' 
          + @TargetTable + N' (' + @IncludedColumns + N')'
          + @FilterDefinition + @Options + @PhysicalLocation

        INSERT INTO @ddl(query) VALUES (@cmd)

        if @Debug = 1 print @cmd

    end
    else --'normal' indexes
    begin
      set @Unique = case when @IsUnique = 1 then N' UNIQUE ' else N'' end
      set @KeyColumns = N''
      set @IncludedColumns = N''

      SELECT @KeyColumns = @KeyColumns + N'[' + c.name + N'] ' + CASE WHEN is_descending_key = 1 THEN N'DESC' ELSE N'ASC' END + N','
      FROM sys.index_columns ic
        join sys.columns c
          on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
      WHERE index_id = @IndexId
        and ic.[object_id] = @ObjectId
        and ic.key_ordinal > 0
      ORDER BY ic.key_ordinal

      SELECT @IncludedColumns = @IncludedColumns + N'[' + c.name + N'],'
      FROM sys.index_columns ic
        join sys.columns c
          on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
      WHERE index_id = @IndexId
        and ic.[object_id] = @ObjectId
        and key_ordinal = 0
      ORDER BY index_column_id

      if len(@KeyColumns) > 0
        set @KeyColumns = left(@KeyColumns, len(@KeyColumns) - 1)

      if len(@IncludedColumns) > 0
        set @IncludedColumns = N' INCLUDE (' + LEFT(@IncludedColumns, LEN(@IncludedColumns) - 1) + N')'

      if @FilterDefinition is null
        set @FilterDefinition = N''
      else
        set @FilterDefinition = N'WHERE ' + @FilterDefinition + N' '

      set @Options = N''

      if @DataCompressionDesc != N'NONE'
        set @Options = N'DATA_COMPRESSION = ' + @DataCompressionDesc

      if @IsIncremental = 1 and @UsePS = 1
      begin
        if @Options > N''
          set @Options += N', '

        set @Options += N'STATISTICS_INCREMENTAL = ON'
      end

      if @Options > N''
        set @Options = N' WITH (' + @Options + N') '

      if @IsPrimaryKey = 1
      begin
        declare @PKName sysname = N'PK_' + replace(replace(replace(@TargetTable, N'.', N'_'), N'[', N''), N']', N'')

        set @cmd = N'ALTER TABLE ' + @TargetTable
          + N' ADD CONSTRAINT ' + @PKName
          + N' PRIMARY KEY (' + @KeyColumns + N')'
          + @Options + @PhysicalLocation

        INSERT INTO @ddl(query) VALUES (@cmd)

        if @Debug = 1 print @cmd
      end
      else if @SkipNCIndexes = 0 or (@SkipNCIndexes = 1 and @IndexType = 1) -- 1 = Clustered
      begin
        set @cmd = N'CREATE '
          + case @IsUnique  when 1 then N'UNIQUE ' else N'' end
          + case @IndexType when 2 then N'NONCLUSTERED ' else N'CLUSTERED ' end
          + N'INDEX [' + @IndexName + N'] ON ' + @TargetTable + N' (' + @KeyColumns + N')'
          + @IncludedColumns + @FilterDefinition + @Options + @PhysicalLocation

        INSERT INTO @ddl(query) VALUES (@cmd)

        if @Debug = 1 print @cmd
      end -- /@SkipNCIndexes check
    end -- /'normal' indexes

    fetch next from indexcursor into @IndexId, @IndexName, @IsUnique, @IsUniqueConstraint, @IsPrimaryKey, @FilterDefinition, @IndexType, @DataCompressionDesc, @FileGroupName, @PSName, @IsIncremental

  end --/index list

  close indexcursor
  deallocate indexcursor

end --@SkipAllIndexes = 0

if @Debug = 0 --run DDL
begin try
  declare
    @id int = 1,
    @cnt int = (select COUNT(*) from @ddl)

  BEGIN TRAN

  while @id <= @cnt
  begin
    select @cmd = query from @ddl where id = @id

    exec(@cmd)

    set @id += 1
  end

  COMMIT TRAN
end try
begin catch
  if xact_state() <> 0
    ROLLBACK TRANSACTION

  ;throw;
end catch
--/run DDL

-- Define the check constraint
/*
if @LoadDay > 0
begin
  set @cmd = N'ALTER TABLE ' + @TargetTable + N' ADD Constraint CK_' + @TargetTable + N'_' + @PartColumn + N' CHECK (' +  @PartColumn + N' = ' + Convert(char(8), @LoadDay) + N')'
  if @Debug = 1 print @cmd
  exec (@cmd)
end
*/

GO
go
/*
Not partitioned tables are supported too.

if @From and/or @To is not specified, function will detect required partitions automatically

  @From is null and EndDateKey is null - all partitions in a table
  @From = X and EndDateKey is null all partitions starting from X, inclusive
  @From = is null and EndDateKey = X all partitions up to X, not inclusive
  @From = X and EndDateKey = Y all partitions in a range [X, Y)

use cases:
1. table-level per-partition maintenance, Action must be REBUILD, non index-specific

exec sspm.Defragment 
  @TableName = 'Sales.CustomerTransactions', 
  @Action       = 'REBUILD'

exec sspm.Defragment 
  @TableName = 'Sales.CustomerTransactions', 
  @From = 20180401,
  @To   = 20180402,
  @Action       = 'REBUILD'

2. PK-only 

exec sspm.Defragment 
  @TableName = 'Sales.CustomerTransactions', 
  @From = 20180401,
  @To   = 20180402,
  @Action       = 'REBUILD',
  @PKOnly       = 1

3. One index only

exec sspm.Defragment 
  @TableName = 'Sales.CustomerTransactions', 
  @From = 20180401,
  @To   = 20180402,
  @Action       = 'REBUILD',
  @IndexName   = N'IX_FCT__SbkBetSettled_BetKEy_BetActionTime_Datekey'

4. AUTO mode. Action is based on actual fragmentation and Threshold
  if fragmentation is < @SkipIfLess, do nothing
  if fragmentation >= @SkipIfLess an < @RebuildFrom - REORGANIZE
  else REBUILD 
  default for @SkipIfLess is 0
  default for @RebuildFrom is 10

exec sspm.Defragment 
  @TableName = 'Sales.CustomerTransactions', 
  @Action       = 'AUTO',
  @SkipIfLess  = 1, --percents
  @RebuildFrom = 10 --percents

*/

CREATE OR ALTER PROCEDURE sspm.Defragment
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
  @PFBaseType varchar(10), --super-type of PF, int-based or date-based
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

set @partitioned = IIF(@PF is null, 0, 1)

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
  /*
  find out @PFBaseType, time-based or int-based
  */
  set @PFBaseType = 
    case 
      when @PFType in (N'date', N'smalldatetime', N'datetime')
        or @PFType like N'time%'
        or @PFType like N'datetime2%'
        or @PFType like N'datetimeoffset%'
      then 'time-based'
      when @PFType in (N'bigint', N'int', N'smallint', N'tinyint')
      then 'int-based'
    end

  if @PFBaseType is null
  begin
    print '@PFType is not recognized, type can be date/time or any integer type'
    print 'Options are '
    print ''
    print 'time-based          int-based'
    print '----------          ---------'
    print 'time[(n)]           bigint'
    print 'date                int'
    print 'smalldatetime       smallint'
    print 'datetime            tinyint'
    print 'datetime2[(n)]'
    print 'datetimeoffset[(n)]'
    raiserror('@PFType is not recognized', 16, 0)
    return
  end

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
    print 'No data, nothing to defragment!'
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
go
go
/*
  This SP can be used to
    1. switch range of partitions from @SourceTable to @TargetTable (appropriate partitions in @TargetTable will be emptied)
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
  raiserror('Source table is NULL!', 16, 1)
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
  raiserror('Source table doesn''t exists, @SourceTable = %s', 16, 1, @SourceTable)
  return
end

if @TargetTable is not null and object_id(@TargetTable, 'U') is null
begin
  raiserror('Target table doesn''t exists, @TargetTable = %s', 16, 1, @TargetTable)
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
  raiserror('Source table is not partitioned!', 16, 1)
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
  raiserror('Range of values should define at least one whole partition to switch!', 16, 1)
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
  --count rows in destination table, if we are moving partitions between sorce and destination
  --count rows in source table if we are moving partitions between source and none
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

if @CleanupOnly = 1 and @RowsDeleted = 0 --Nothing to delete, source table is empty
begin
  if @Debug > 0
  begin
    raiserror('Nothing to delete, source table is empty', 0, 0) with nowait
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
    if @Debug > 0 print N'Target table has data, let''s check destination temp table, @TgtTempTable = ' + @TgtTempTable
    set @Table = @TargetTable
    set @TempTable = @TgtTempTable
  end
  else
  begin
    if @Debug > 0 print N'Source table has data, let''s check dest table, @SrcTempTable = ' + @SrcTempTable
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
      if @Debug > 0 print 'Found not-empty partition in source table'
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
go
