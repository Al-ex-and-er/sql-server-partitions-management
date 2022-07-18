--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
/*
  This procedure creates partition function (PF) and (optionally) partition schema (PS).

  This procedure creates PF/PS for a data warehouse keys, like DateKey, MonthKey etc.

  DateKey is a date expressed as integer. '2020-01-01' = 20200101

  @KeyType describes a type of a key
    DateKey 20200101
    MonthKey 202001
    YearKey 2020

  Partition function starts at @Start and then makes increments (@Step) till it reaches @Stop. Range is inclusive [@Start, @Stop]

  All partitions can be placed in the same file group (@PSAllTo) or use a layout from configuration table (@PSLayout)

  Partition function can be based only on integer type. 
  
  @Step can be
    2 - it is an interval, defined as a string (PostreSQL-inspired)
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
  
  exec sspm.CreateDW
    @PFName   = 'pf_int',
    @Start    = 20200101,
    @Stop     = 20210101,
    @Step     = '1 day',
    @PrintOnly= 1
*/

CREATE or alter PROCEDURE sspm.CreateDW
(
  @PFName    sysname,
  @Start     int,
  @Stop      int,
  @Step      varchar(50),
  @KeyType   varchar(10),
  @PFRange   varchar(5) = 'RIGHT', --right or left
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

set @KeyType = nullif(trim(@KeyType), '')

if @KeyType not in ('DateKey', 'MonthKey', 'YearKey')
begin
  print '@KeyType can be DateKey, MonthKey or YearKey'
  raiserror('@KeyType is wrong', 16, 0)
  return
end

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

  set @PSAllTo  = nullif(trim(@PSAllTo) , '')
  set @PSLayout = nullif(trim(@PSLayout), '')

  if @PSAllTo is not null and substring(@PSAllTo, 1, 1) <> '['
    set @PSAllTo = quotename(@PSAllTo)

  if @PSAllTo is not null and @PSLayout is not null
  begin
    set @msg = 'Only one parameter must be specified, @PSAllTo or @PSLayout!'
    raiserror(@msg, 16, 0)
    return
  end
  else if @PSAllTo is null and @PSLayout is null
  begin
    set @msg = '@PSAllTo or @PSLayout must be specified!'
    raiserror(@msg, 16, 0)
    return
  end
end
else --@PSName is null
begin
  if @PSAllTo is not null or @PSLayout is not null
  begin
    set @msg = '@PSAllTo and @PSLayout can be used only when @PSName is specified!'
    raiserror(@msg, 16, 0)
    return
  end
end

-- functions to convert 
--   Key to date (@key2dt): 20200101 (int) -> '2020-01-01' (date)
--   Date to a Key's string representation (@dt2str): '2020-01-01' (date) -> '20200101'
declare
  @key2dt_params nvarchar(500) = N'@KeyType varchar(10), @x int, @res date output',
  @key2dt_sql nvarchar(600) =
'SELECT @res = cast(
  case @KeyType
    when ''DateKey''  then cast(@x as varchar(8))
    when ''MonthKey'' then cast(@x as varchar(6))+''01''
    when ''YearKey''  then cast(@x as varchar(4))+''0101''
  end
  as date)'

declare
  @dt2str_params nvarchar(500) = N'@KeyType varchar(10), @x date, @res varchar(8) output',
  @dt2str_sql nvarchar(600) =
'SELECT @res = 
  case 
    when @KeyType = ''DateKey''  then  convert(varchar(8), @x, 112)
    when @KeyType = ''MonthKey'' then  convert(varchar(6), @x, 112)
    when @KeyType = ''YearKey''  then  convert(varchar(4), @x, 112)
  end'

--check @Start/@Stop/@Step
--we should be able to cast @Start and @Stop to the target type
declare 
  @sql nvarchar(600),
  @params nvarchar(500),
  @StartDT date,
  @StopDT date

begin try
  exec sp_executesql @key2dt_sql, @key2dt_params, @KeyType = @KeyType, @x = @Start, @res = @StartDT output
  exec sp_executesql @key2dt_sql, @key2dt_params, @KeyType = @KeyType, @x = @Stop , @res = @StopDT  output
end try
begin catch
   print 'Can''t convert @Start or @Stop to date'
   print error_message()
   return
end catch

if @Start > @Stop
begin
  raiserror('@Start can''t be > than @Stop!', 16, 0)
  return
end

declare
  @step_second int = 0, 
  @step_minute int = 0, 
  @step_hour   int = 0, 
  @step_day    int = 0, 
  @step_week   int = 0, 
  @step_month  int = 0, 
  @step_year   int = 0

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

--replace two spaces with one space
while patindex('%  %', @interval) > 0
begin
  set @interval = replace(@interval, '  ', ' ')
end

--catch errors like '1day' (no spaces)
if exists
(
  SELECT 1
  FROM string_split(@interval, ' ') t
  where value not in ('second', 'minute', 'hour', 'day', 'week', 'month', 'year')
    and isnumeric(value) = 0
)
begin
  raiserror('Error in @Step! It should be a ''quantity unit'', like ''1 day''', 16, 0)
  return
end

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
  
if @step_second    is null
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

if @step_hour > 0 or @step_minute > 0 or @step_second > 0
begin
  raiserror('This step is not supported!', 16, 0)
  return
end

--if we have a week increment, first day should be Monday, only for keys
--if @step_week > 0 and (datepart(weekday, @Start) + 5) % 7 + 1 <> 1
--begin
--  raiserror('@Step is not in correct format', 16, 0)
--  return
--end 

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
  point int not null,
  pointStr varchar(30) not null,
  FGName sysname null
)

declare 
  @list varchar(max) = '',
  @point int,
  @pointDT date = @StartDT,
  @pointStr varchar(8),
  @maxN int = 15000 --max number of partitions


while @pointDT <= @StopDt and @maxN >= 0
begin
  --convert to string
  exec sp_executesql @dt2str_sql, @dt2str_params, @KeyType = @KeyType, @x = @pointDT, @res = @pointStr output
  set @point = cast(@pointStr as int)

  INSERT INTO #points(point, pointStr) VALUES (@point, '''' + @pointStr + '''')

  set @list = @list + @pointStr + ', ' 
  --increment
  if @step_day   <> 0 set @pointDT = dateadd(day  , @step_day  , @pointDT)
  if @step_week  <> 0 set @pointDT = dateadd(week , @step_week , @pointDT)
  if @step_month <> 0 set @pointDT = dateadd(month, @step_month, @pointDT)
  if @step_year  <> 0 set @pointDT = dateadd(year , @step_year , @pointDT)
  
  set @maxN -= 1
end --while

if @maxN < 0
begin
  raiserror('Maximum number of partitions reached', 16, 0)
  return
end

if len(@list) > 2
  set @list = substring(@list, 1, len(@list) - 1)

declare @cmd varchar(max) = 'CREATE PARTITION FUNCTION ' + quotename(@PFName) + '(int) AS RANGE ' + @PFRange + ' FOR VALUES (' + @list + ')'

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

--
-- PS
--
if @PSName is not null --let's create PS
begin
  if @PSAllTo is not null
  begin
    set @cmd = 'CREATE PARTITION SCHEME ' + @PSName + ' AS PARTITION ' + @PFName + ' ALL TO (' + @PSAllTo + ')'
  end --/@PSAllTo is not null
  else --@PSAllTo is null, use sspm.PS_Layout
  begin
    UPDATE t
    SET t.FGName = trim(l.FGName)
    FROM #points t
      join sspm.PS_Layout l
        on
        ( @PFRange = 'RIGHT' 
          and ( (l.PointFrom is null and t.point < l.PointTo)
                or (t.point >= l.PointFrom and t.point < l.PointTo)
                or (t.point >= l.PointFrom and l.PointTo is null)
              )
        )
        or
        ( @PFRange = 'LEFT' 
          and ( (l.PointFrom is null and t.point <= l.PointTo)
                or (t.point > l.PointFrom and t.point <= l.PointTo)
                or (t.point > l.PointFrom and l.PointTo is null)
              )
        )
    WHERE l.LayoutName = @PSLayout

    declare @notAssignedPoint int = (select top 1 point from #points where FGName is null)

    if @notAssignedPoint is not null
    begin
      set @pointStr = cast(@notAssignedPoint as varchar(30))

      set @msg = 'Can''t find filegroup for point ' + CONVERT(varchar(32), @pointStr, 121) + ' !' 
      raiserror(@msg, 16, 0)
      return
    end

    UPDATE t
    SET FGName = quotename(FGName)
    FROM #points t
    WHERE substring(FGName, 1, 1) <> '['

    declare @ExtraFG sysname

    if @PFRange = 'RIGHT'
      SELECT @ExtraFG = FGName
      FROM sspm.PS_Layout
      WHERE LayoutName = @PSLayout
        and PointFrom is null
    else 
      SELECT @ExtraFG = FGName
      FROM sspm.PS_Layout
      WHERE LayoutName = @PSLayout
        and PointTo is null

    if @ExtraFG is null
    begin
      set @msg = 
        case @PFRange 
          when 'RIGHT' then 'The leftmost' 
          else 'The rightmost' 
        end + ' file group is not defined in the layout '
        + @PSLayout

      raiserror(@msg, 16, 0)
    end
    else if substring(@ExtraFG, 1, 1) <> '['
    begin
      set @ExtraFG = quotename(@ExtraFG)
    end

    if @PFRange = 'RIGHT'
      set @list = @ExtraFG + ', '
    else
      set @list = ''  

    SELECT @list = @list + t.FGName + ', '
    FROM #points t
    ORDER BY id

    if @PFRange = 'LEFT'
      set @list = @list + @ExtraFG + ', '

    if len(@list) > 2
      set @list = substring(@list, 1, len(@list) - 1)

    set @cmd = 'CREATE PARTITION SCHEME ' + @PSName + ' AS PARTITION ' + @PFName + ' TO (' + @list + ')'

    if @cmd is null
    begin
      raiserror('Failed to build CREATE PARTITION SCHEME statement!', 16, 0)
      return
    end

  end --if

  if @PrintOnly = 1
    if len(@cmd) <= 4000
      print @cmd
    else 
      SELECT CAST('<root><![CDATA[' + @cmd + ']]></root>' AS XML)
  else
    exec (@cmd)

end--create PS
go
