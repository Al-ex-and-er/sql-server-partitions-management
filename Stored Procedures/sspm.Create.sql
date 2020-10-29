/*
  This procedure creates partition function (PF) and (optionally) partition schema (PS).

  Partition function starts at @Start and then makes increments (@Step) till it reaches @Stop. Range is inclusive [@Start, @Stop]

  All partitions can be placed in the same file group (@AllToFilegroup) or use a layout from configuration table (@PSLayout)

  Partition function can be based on any integer and date time type. 
  
  @ArgType can be:

    for dates and date keys:

    Hourly
    Daily               
    Weekly
    Monthly
    Quarterly
    HalfYearly
    Yearly

    for numbers

    Number

  @DataType can be:

    time-based          int-based
    ----------          ---------
    time[(n)]           bigint
    date                int
    smalldatetime       smallint
    datetime            tinyint
    datetime2[(n)]
    datetimeoffset[(n)]

  @Step can be

    Hour
    Day
    Week
    Month
    Quarter
    HalfYear
    Year

  @PFRange can be RIGHT or LEFT.

  @PrintOnly = 1 doesn't run commands but prints them

  @PSName - if null, PS won't be created
  @PSAllTo - name of a filegroup where to place all partitions
  @PSLayout - name of a layout

  Layout of partitions can be specified in the sspm.PS_Layout table.

  sspm.PS_Layout(LayoutName, PointFrom, PointTo, FGName)

  for example

  ('L1',  1, 10, 'FG1'),
  ('L1', 10, 20, 'FG2')
  
  exec sspm.CreatePF
    @PFName   = 'pf_int',
    @ArgType  = 'number',
    @Start    = 0,
    @Stop     = 100,
    @Step     = 10,
    @DataType = 'int',
    @PrintOnly= 1
*/
CREATE PROCEDURE sspm.CreatePF
(
  @PFName    sysname,
  @ArgType   varchar(12),
  @Start     sql_variant,
  @Stop      sql_variant,
  @Step      sql_variant,
  @PFRange   varchar(5) = 'RIGHT', --right or left
  @DataType  varchar(50),
  @PSName    sysname = null,
  @PSAllTo   sysname = null, -- name of the file group
  @PSLayout  varchar(50) = null, -- name of the layout from the configuration table
  @PrintOnly bit = 0
)
as
set nocount on
--
-- Check parameters
--
if @PFName is null
begin
  raiserror('@PFName must be defined', 16, 0)
  return
end

declare @msg varchar(1024)

if exists (select top 1 1 from sys.partition_functions where name = @PFName) and @PrintOnly = 0
begin
  set @msg = 'Partition function ' + @PFName + ' already exists!'
  raiserror(@msg, 16, 0)
  return
end

if @ArgType not in ('Hourly', 'Daily', 'Weekly', 'Monthly', 'Quarterly', 'HalfYearly', 'Yearly', 'Number')
begin
  raiserror('@ArgType is wrong', 16, 0)
  return
end

set @PFRange = upper(ltrim(rtrim(@PFRange)))

if @PFRange not in ('RIGHT', 'LEFT')
begin
  print '@PFRange can be RIGHT or LEFT'
  raiserror('@PFRange is wrong', 16, 0)
  return
end

set @PFName = nullif(ltrim(rtrim(@PFName)), '')
set @PSName = nullif(ltrim(rtrim(@PSName)), '')

if @PSName is not null
begin
  if exists (select 1 from sys.partition_schemes where name = @PSName) and @PrintOnly = 0
  begin
    set @msg = 'Partition schema ' + @PSName + ' already exists!'
    raiserror(@msg, 16, 0)
    return
  end

  if @PSAllTo is not null and @PSLayout is not null
  begin
    set @msg = 'Only one parameter must be specified, @PSAllTo or @PSLayout!'
    raiserror(@msg, 16, 0)
    return
  end
end
else --@PSName is null
begin
  if @PSAllTo is not null or @PSLayout is not null
  begin
    set @msg = '@PSAllTo and @PSLayout can be used when @PSName is specified!'
    raiserror(@msg, 16, 0)
    return
  end
end

/*
find out @BaseType, time-based or int-based
*/
declare @BaseType varchar(20) = null

if @ArgType != 'Number' 
begin
  if @DataType !='timestamp' and (@DataType like '%time%' or @DataType = 'date')
  begin
    set @BaseType = 'time-based date'
  end

  if @DataType in ('bigint', 'int', 'smallint', 'tinyint')
  begin
    set @BaseType = 'int-based date'
  end
end
else
begin
  if @DataType = 'bit'
  begin
    raiserror('@DataType can''t be a bit!', 16, 0)
    return
  end
  set @BaseType = 'number'
  /*
    numeric, decimal,  
    tinyint, smallint, int,
    smallmoney, money
  */
end

if @BaseType is null
begin
  raiserror('@DataType is not recognized, type can be date/time or any numeric type', 16, 0)
  return
end

--check @Step
if @BaseType = 'time-based date' and @Step not in ('Hour', 'Day', 'Week', 'Month', 'Quarter', 'HalfYear', 'Year')
begin
  print 'For time-based types these steps are supported: Hour, Day, Week, Month, Quarter, HalfYear and Year'
  raiserror('@Step is not recognized', 16, 0)
  return
end 

if @BaseType = 'int-based date' and @Step not in ('Day', 'Week', 'Month', 'Quarter', 'HalfYear', 'Year')
begin
  print 'For int-based date types we support these steps: Day, Week, Month, Quarter, HalfYear, Year'
  raiserror('@Step is not recognized', 16, 0)
  return
end

if @BaseType = 'int-based date' and isnull(@ArgType, '') = 'Hourly'
begin
  print 'Int-based date types can''t be Hourly'
  raiserror('@ArgType is not recognized', 16, 0)
  return
end

--check @Start and @Stop
declare 
  @vStart sql_variant, 
  @vStop sql_variant,
  @castSQL varchar(200) = 'select @res = try_cast(@v as ' + @DataType + ')'

exec sp_executesql @sql, N'@v sql_variant, @res sql_variant out', @Start, @vStart out
exec sp_executesql @sql, N'@v sql_variant, @res sql_variant out', @Stop , @vStop  out

if @vStart is null
begin
  set @msg = '@Start can''t be converted to ' + @DataType + '!' 
  raiserror(@msg, 16, 0)
  return
end

if @vStop is null
begin
  set @msg = '@Stop can''t be converted to ' + @DataType + '!' 
  raiserror(@msg, 16, 0)
  return
end

if @vStart > @vStop
begin
  raiserror('@Start can''t be > than @Stop!', 16, 0)
  return
end

--
-- end of parameters validation
--

--
-- PF
--
if object_id('tempdb..#points') is not null
  DROP TABLE #points

CREATE TABLE #points
( 
  id int not null identity(1,1),
  point sql_variant not null,
  pointStr varchar(30) not null,
  FGName sysname null
)

declare 
  @list varchar(max) = '',
  @direction int = case @PFRange when 'RIGHT' then -1 else 1 end

if @BaseType = 'time-based date' --set @list
begin
  declare 
    @curDT datetime2(7) = @vStart,
    @pointStr varchar(50)

  while @curDT <= @vStop
  begin
    set @pointStr = convert(varchar(19), @curDT, 121)

    INSERT INTO #points(pointDT, pointStr) VALUES (@curDT, @pointStr)

    set @list = @list + '''' + @pointStr + ''', ' 
    set @curDT = dbo.DateCheckpointAddRange(@curDT, @checkpoint_type, 1, @Step)
  end --while
end
else --int-based, set @list
begin
  declare 
    @curI bigint = @vStart, 
    @new_curI bigint,
    @valI varchar(25),
    @valIDT datetime2(7)

  while @curI <= @vStop 
  begin
    
    set @pointStr = convert(varchar(25), @curI)

    set @valIDT = dbo.convert_KeyToDate(@curI, @checkpoint_type)

    INSERT INTO #points(pointDT, pointStr) VALUES (@valIDT, @pointStr)

    set @list = @list + @pointStr + ', ' 

    set @new_curI = dbo.KeyAddRange(@curI, @checkpoint_type, 1, @Step)

    if @curI <> @new_curI --if one of Add functions raised an error, @new_curI = @curI
    begin
      set @curI = @new_curI
    end
    else
    begin
      raiserror('Failed to increment @curI!', 16, 0)
      return
    end
  end --while
end --/set @list

if len(@list) > 2
  set @list = substring(@list, 1, len(@list) - 1)

declare @cmd varchar(max) = 'CREATE PARTITION FUNCTION ' + @PFName + '(' + @DataType +') AS RANGE ' + @PFRange + ' FOR VALUES (' + @list + ')'

if @cmd is null
begin
  raiserror('Failed to build CREATE PARTITION FUNCTION statement!', 16, 0)
  return
end

if @PrintOnly = 1
  print @cmd
else
  exec (@cmd)

if not exists(select 1 from dba.PF_Function where pf_name = @PFName) and @PrintOnly = 0
begin
  INSERT INTO dba.PF_Function (pf_name, checkpoint_type, step) values (@PFName, @checkpoint_type, @Step)
end

--
-- PS
--
if @PSName is not null --let's create PS
begin
  UPDATE t
  SET t.FGName = f.FGName
  FROM #points t
    join dba.PF_FileGroup_Config f
      on  (@PFRange = 'RIGHT' and t.pointDT >= DateFrom and t.pointDT <  DateTo)
       or (@PFRange = 'LEFT'  and t.pointDT >  DateFrom and t.pointDT <= DateTo)

  declare @notAssignedPoint datetime2(7)
  set @notAssignedPoint = (select top 1 pointDT from #points where FGName is null)

  if @notAssignedPoint is not null
  begin
    set @msg = 'Can''t find filegroup for point ' + CONVERT(varchar(32), @notAssignedPoint, 121) + ' !' 
    raiserror(@msg, 16, 0)
    return
  end

  declare @ExtraFG sysname = 
    case @PFRange
      when 'RIGHT' then 'FG_HIST'
      when 'LEFT' then 'FG_HEAD'
    end

  set @list = ''

  if @PFRange = 'RIGHT' 
    set @list = '[' + @ExtraFG + '], '

  SELECT @list = @list + '[' + t.FGName + '], '
  FROM #points t
  ORDER BY id

  if @PFRange = 'LEFT' 
    set @list = @list + '[' + @ExtraFG + '], '

  if len(@list) > 2
    set @list = substring(@list, 1, len(@list) - 1)

  set @cmd = 'CREATE PARTITION SCHEME ' + @PSName + ' AS PARTITION ' + @PFName + ' TO (' + @list + ')'

  if @cmd is null
  begin
    raiserror('Failed to build CREATE PARTITION SCHEME statement!', 16, 0)
    return
  end

  if @PrintOnly = 1
    print @cmd
  else
    exec (@cmd)
end --create PS
go
