CREATE OR ALTER PROCEDURE sspm.UpdateStats
(
  @TableName sysname,
  @From sql_variant = NULL,
  @To   sql_variant = NULL,
  @Debug bit = 0
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

declare @cmd nvarchar(max)

if @partitioned = 0
begin
  set @cmd = ''
end
else
begin
  declare @CurPartNum int = @MinPartNum

end

go
