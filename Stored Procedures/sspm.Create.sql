/*
  This procedure creates partition function (PF) and (optionally) partition schema (PS).

  Partition function starts at @Start and then makes increments (@Step) till it reaches @Stop. Range is inclusive [@Start, @Stop]

  All partitions can be placed in the same file group (@PSAllTo) or use a layout from configuration table (@PSLayout)

  Partition function can be based on any integer and date time type. 
  
  @ArgType can be:

    for date keys:

    DateKey               
    WeekKey
    MonthKey
    YearKey

    for dates

    'Time-based'

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
    1 - for numbers, it is just a single number, increment
    2 - for time-based, it is an interval, defined as a string (PostreSQL-inspired)
      quantity unit [quantity unit...]
      where 
        quantity is an integer (can be negative)
        and unit is a string 'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
      examples '1 hour', '30 minutes'

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

CREATE or alter PROCEDURE sspm.CreatePF
(
  @PFName    sysname,
--  @ArgType   varchar(12),
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
set @PFName = nullif(trim(@PFName), '')
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

--if @ArgType not in ('DateKey', 'WeeklKey', 'MonthKey', 'YearKey', 'Time-based', 'Number')
--begin
--  raiserror('@ArgType is wrong', 16, 0)
--  return
--end

set @PFRange = upper(ltrim(rtrim(@PFRange)))

if @PFRange not in ('RIGHT', 'LEFT')
begin
  print '@PFRange can be RIGHT or LEFT'
  raiserror('@PFRange is wrong', 16, 0)
  return
end

set @PSName = nullif(trim(@PSName), '')

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

if @DataType in ('date', 'datetime', 'smalldatetime')
  or (@DataType like 'time%' and @DataType <> 'timestamp')
  or @DataType like 'datetime2%'
  or @DataType like 'datetimeoffset%'
begin
  set @BaseType = 'time-based'
end
else if @DataType in 
  (
    'bigint', 'int', 'smallint', 'tinyint', 
    'smallmoney', 'money',
    'real'
--    'bit'
  )
  or @DataType like 'numeric%'
  or @DataType like 'decimal%'
  or @DataType like 'float%'
begin
  set @BaseType = 'Numeric'
end

if @BaseType is null
begin
  raiserror('@DataType is not recognized, type can be date/time or any numeric type (but not bit)', 16, 0)
  return
end

--check @Start/@Stop/@Step
--we should be able to cast @Start and @Stop to the target type
declare @sql nvarchar(600) = 'select @res = try_cast(@x as ' + @DataType +')'

begin try
  exec sp_executesql @sql, N'@x sql_variant, @res sql_variant output', @x = @Start, @res = @Start output
  exec sp_executesql @sql, N'@x sql_variant, @res sql_variant output', @x = @Stop, @res = @Stop output
end try
begin catch
   print 'Can''t convert @Start or @Stop to ' + @DataType
   print error_message()
   return
end catch

set @msg = ''
if @Start is null
begin
  set @msg = '@Start can''t be converted to ' + @DataType
  raiserror(@msg, 16, 0)
  return
end else if @Stop is null
begin
  set @msg = '@Stop can''t be converted to ' + @DataType
  raiserror(@msg, 16, 0)
  return
end

if @Start > @Stop
begin
  raiserror('@Start can''t be > than @Stop!', 16, 0)
  return
end

if @BaseType = 'numeric' --for numeric types @Step should be of the same type
begin
  exec sp_executesql @sql, N'@x sql_variant, @res sql_variant output', @x = @Step, @res = @Step output

  set @msg = ''
  if @Step is null
  begin
    set @msg = '@Step can''t be converted to ' + @DataType
    raiserror(@msg, 16, 0)
    return
  end
end
else if @BaseType = 'time-based' --for time-based types @Step is a string
begin
  declare @StepType sysname = cast(sql_variant_property(@Step, 'BaseType') as sysname)

  if @StepType not in ('varchar', 'nvarchar')
  begin
    raiserror('@Step must be a string describing interval', 16, 0)
    return
  end
end


declare
  @step_second int = 0, 
  @step_minute int = 0, 
  @step_hour   int = 0, 
  @step_day    int = 0, 
  @step_week   int = 0, 
  @step_month  int = 0, 
  @step_year   int = 0

if @BaseType = 'time-based' --parse interval
begin
  --replace plurals
  declare @interval varchar(300) = 
    replace(
      replace(
        replace(
          replace(
            replace(
              replace(replace(cast(@Step as varchar(300)), 'seconds', 'second'),
              'minutes', 'minute'),
            'hours', 'hour'),
          'days', 'day'),
        'weeks', 'week'),
      'months', 'month'),
    'years', 'year')

  ;with parts as
  (
    SELECT quantity = cast(quantity as int), unit
    FROM 
    (
      SELECT quantity = value, unit = lead(value) over(order by (select 1))
      FROM string_split(@interval, ' ') t
    ) s
    WHERE isnumeric(quantity) = 1
  )
  SELECT 
    @step_second = [second], 
    @step_minute = [minute], 
    @step_hour   = [hour], 
    @step_day    = [day], 
    @step_week   = [week], 
    @step_month  = [month], 
    @step_year   = [year]
  FROM parts p
    PIVOT (min(quantity) for unit in ([second], [minute], [hour], [day], [week], [month], [year])) t
  
  if @step_second     is null
     and @step_minute is null
     and @step_hour   is null
     and @step_day    is null
     and @step_week   is null
     and @step_month  is null
     and @step_year   is null
  begin
    raiserror('@Step is not in correct format', 16, 0)
    return
  end

  if @DataType = 'date' and (@step_hour > 0 or @step_minute > 0 or @step_second > 0)
  begin
    raiserror('This step is not supported for date!', 16, 0)
    return
  end

  if @DataType like 'time%' and (@step_day > 0 or @step_week > 0 or @step_month > 0 or @step_year > 0)
  begin
    raiserror('This step is not supported for time!', 16, 0)
    return
  end
  --if we have a week increment, first day should be Monday, only for keys
  --if @step_week > 0 and (datepart(weekday, @Start) + 5) % 7 + 1 <> 1
  --begin
  --  raiserror('@Step is not in correct format', 16, 0)
  --  return
  --end 
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
  @point sql_variant = @Start,
  @pointStr varchar(50),
  @params nvarchar(500),
  @maxN int = 15000 --max number of partitions

if @BaseType = 'Numeric'
begin
  set @sql = N'select @res = cast( cast(@point as ' + @DataType + N') + cast(@step as ' + @DataType + N') as sql_variant)'
  set @params = N'@point sql_variant, @Step sql_variant, @res sql_variant output'
end
else
begin
  set @sql = N'declare @t ' + @DataType + N' = cast(@point as ' + @DataType + N')
if @step_second <> 0 set @t = dateadd(second, @step_second, @t)
if @step_minute <> 0 set @t = dateadd(minute, @step_minute, @t)
if @step_hour   <> 0 set @t = dateadd(hour  , @step_hour  , @t)
if @step_day    <> 0 set @t = dateadd(day   , @step_day   , @t)
if @step_week   <> 0 set @t = dateadd(week  , @step_week  , @t)
if @step_month  <> 0 set @t = dateadd(month , @step_month , @t)
if @step_year   <> 0 set @t = dateadd(year  , @step_year  , @t)
select @res = cast(@t as sql_variant)
'
  set @params = N'@point sql_variant, @step_second int, @step_minute int, @step_hour int, @step_day int, @step_week int, @step_month int, @step_year int, @res sql_variant output'
end  

while @point <= @Stop and @maxN >= 0
begin
  --convert to string
  set @pointStr =
  case 
    when @DataType in('date', 'datetime')
      or @DataType like 'datetime2%'
      or @DataType like 'datetimeoffset%'
      or @DataType like 'time%' 
        then '''' + convert(varchar(30), @point, 121) + ''''
    when @DataType = 'smalldatetime' 
      then '''' + cast(convert(varchar(30), @point, 121) as varchar(19)) + ''''
    else cast(@point as varchar(30))
  end

  INSERT INTO #points(point, pointStr) VALUES (@point, @pointStr)

  set @list = @list + @pointStr + ', ' 
  --increment
  if @BaseType = 'Numeric'
  begin
    --set @point += @Step
    exec sp_executesql @sql, @params, @point = @point, @Step = @Step, @res = @point output
  end
  else --@BaseType = 'time-based'
  begin
    exec sp_executesql @sql, @params, 
    @point       = @point, 
    @step_second = @step_second,
    @step_minute = @step_minute,
    @step_hour   = @step_hour  ,
    @step_day    = @step_day   ,
    @step_week   = @step_week  ,
    @step_month  = @step_month ,
    @step_year   = @step_year  ,
    @res         = @point output
  end

  set @maxN -= 1
end --while

if @maxN < 0
begin
  raiserror('Maximum number of partitions reached', 16, 0)
  return
end

if len(@list) > 2
  set @list = substring(@list, 1, len(@list) - 1)

declare @cmd varchar(max) = 'CREATE PARTITION FUNCTION ' + quotename(@PFName) + '(' + @DataType +') AS RANGE ' + @PFRange + ' FOR VALUES (' + @list + ')'

if @cmd is null
begin
  raiserror('Failed to build CREATE PARTITION FUNCTION statement!', 16, 0)
  return
end

if @PrintOnly = 1
  if len(@cmd) <= 4000
    print @cmd
  else 
    SELECT CAST('<root><![CDATA[' + @cmd + ']]></root>' AS XML)
else
  exec (@cmd)
/*
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
*/
go
