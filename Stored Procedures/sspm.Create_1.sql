/*
  This procedure creates partition function (PF) and (optionally) partition schema (PS).

  All partitions can be placed in the same file group (@all_to_filegroup) or use a layout from configuration table (@layout_name)
  Layout of partitions can be found in the sspm.PS_Layout table.

  sspm.PS_Layout(LayoutName, PointFrom, PointTo, FGName)

  for example

  ('L1',  1, 10, 'FG1'),
  ('L1', 10, 20, 'FG2')

  Partition function can be based on any integer and date time type. 
  
  @data_type can be:

  time-based          int-based
  ----------          ---------
  time[(n)]           bigint
  date                int
  smalldatetime       smallint
  datetime            tinyint
  datetime2[(n)]
  datetimeoffset[(n)]

  @checkpoint_type is required for ineger-based functions and specifies what kind of data INT contains (DateKey, MonthKey etc)

  Partition function starts at @start and then makes increments (@step) till it reaches @stop. Range is inclusive [@start, @stop]
  @step can be
  time-based  int-based 
  ----------  ---------
  Hourly      
  Daily       date_sk    
  Weekly      week_sk    
  Monthly     month_sk   
  Quarterly   quarter_sk 
  HalfYearly  halfyear_sk
  Yearly      year_sk    

  @range can be RIGHT or LEFT. Please don't create LEFT functions without clear necessity.
  @print_only don't run commands but prints them
  @ps_name - if null, PS won't be created

*/
CREATE PROCEDURE dba.CreatePF
(
  @pf_name    sysname,
  @start      sql_variant,
  @stop       sql_variant,
  @range      varchar(5) = 'RIGHT', --right or left
  @data_type  varchar(50),
  @all_to_filegroup sysname, --name of the file group
  @layout_name varchar(50), -- name of the layout from the configuration table
  @checkpoint_type varchar(20) = NULL,
  @step       varchar(20),
  @ps_name    sysname = null,
  @print_only bit = 0
)
as
set nocount on
--
-- Check parameters
--
set @range = upper(ltrim(rtrim(@range)))

if @range not in ('RIGHT', 'LEFT')
begin
  print '@range can be only RIGHT or LEFT'
  raiserror('@range is wrong', 16, 0)
  return
end

set @pf_name = nullif(ltrim(rtrim(@pf_name)), '')
set @ps_name = nullif(ltrim(rtrim(@ps_name)), '')

if @pf_name is null
begin
  raiserror('@pf_name must be defined', 16, 0)
  return
end

declare @msg varchar(1024)

if exists (select top 1 1 from sys.partition_functions where name = @pf_name) and @print_only = 0
begin
  set @msg = 'Partition function ' + @pf_name + ' already exists!'
  raiserror(@msg, 16, 0)
  return
end

if @ps_name is not null and exists (select 1 from sys.partition_schemes where name = @ps_name) and @print_only = 0
begin
  set @msg = 'Partition schema ' + @ps_name + ' already exists!'
  raiserror(@msg, 16, 0)
  return
end

/*
find out @base_type, time-based or int-based
*/
declare @base_type varchar(10) = null

if @data_type in ('date', 'smalldatetime', 'datetime')
  or @data_type like 'time%'
  or @data_type like 'datetime2%'
  or @data_type like 'datetimeoffset%'
begin
  set @base_type = 'time-based'
end

if @data_type in ('bigint', 'int', 'smallint', 'tinyint')
begin
  set @base_type = 'int-based'
end

if @base_type is null
begin
  print '@data_type is not recognized, type can be date and time or any integer type'
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
  raiserror('@data_type is not recognized', 16, 0)
  return
end

--check @step
if @base_type = 'time-based' and @step not in ('Hourly', 'Daily', 'Weekly', 'Monthly', 'Quarterly', 'HalfYearly', 'Yearly')
begin
  print 'For time-based types we support these steps: Hourly, Daily, Weekly, Monthly, Quarterly, HalfYearly and Yearly'
  raiserror('@step is not recognized', 16, 0)
  return
end 

if @base_type = 'time-based' and @checkpoint_type is null
  set @checkpoint_type = 'datetime'

if @base_type = 'time-based' and @checkpoint_type <> 'datetime'
begin
  set @checkpoint_type = 'datetime'
  print '@checkpoint_type must be ''datetime'' for time-based PFs'
end

if @base_type = 'int-based' and @step not in ('date_sk', 'week_sk', 'month_sk', 'quarter_sk', 'halfyear_sk', 'year_sk')
begin
  print 'For int-based types we support these steps: date_sk, week_sk, month_sk, quarter_sk, halfyear_sk, year_sk'
  raiserror('@step is not recognized', 16, 0)
  return
end

if @base_type = 'int-based' and isnull(@checkpoint_type, '') not in ('date_sk', 'week_sk', 'month_sk', 'quarter_sk', 'halfyear_sk', 'year_sk')
begin
  print 'For int-based types we support these @checkpoint_type: date_sk, week_sk, month_sk, quarter_sk, halfyear_sk, year_sk'
  raiserror('@checkpoint_type is not recognized', 16, 0)
  return
end

--check @start and @stop
declare @startDT datetime2(7), @stopDT datetime2(7)
declare @startI bigint, @stopI bigint

if @base_type = 'time-based'
begin
  set @startDT = try_cast(@start as datetime2(7))
  set @stopDT  = try_cast(@stop as datetime2(7))

  if @startDT is null
  begin
    raiserror('@start can not be converted to datetime2(7)!', 16, 0)
    return
  end

  if @stopDT is null
  begin
    raiserror('@stop can not be converted to datetime2(7)!', 16, 0)
    return
  end

  if @startDT > @stopDT
  begin
    raiserror('@start can''t be > than @stop!', 16, 0)
    return
  end
end
else --int-based
begin
  set @startI = try_cast(@start as bigint)
  set @stopI  = try_cast(@stop as bigint)

  if @startI is null
  begin
    raiserror('@start can not be converted to bigint!', 16, 0)
    return
  end

  if @stopI is null
  begin
    raiserror('@stop can not be converted to bigint!', 16, 0)
    return
  end

  if @startI > @stopI
  begin
    raiserror('@start can''t be > than @stop!', 16, 0)
    return
  end

  declare @err bit = 0
  begin try
    set @startDT = dbo.convert_KeyToDate(@startI, @checkpoint_type)
    set @stopDT = dbo.convert_KeyToDate(@stopI, @checkpoint_type)
  end try
  begin catch
    set @err = 1
  end catch

  if @err = 1
  begin
    raiserror('@start or @stop is not a valid @checkpoint_type!', 16, 0)
    return
  end

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
  pointDT datetime2(7) not null,
  pointStr varchar(20) not null,
  FGName sysname null
)

declare 
  @list varchar(max) = '',
  @direction int = case @range when 'RIGHT' then -1 else 1 end

if @base_type = 'time-based' --set @list
begin
  declare 
    @curDT datetime2(7) = @startDT,
    @pointStr varchar(50)

  while @curDT <= @stopDT
  begin
    set @pointStr = convert(varchar(19), @curDT, 121)

    INSERT INTO #points(pointDT, pointStr) VALUES (@curDT, @pointStr)

    set @list = @list + '''' + @pointStr + ''', ' 
    set @curDT = dbo.DateCheckpointAddRange(@curDT, @checkpoint_type, 1, @step)
  end --while
end
else --int-based, set @list
begin
  declare 
    @curI bigint = @startI, 
    @new_curI bigint,
    @valI varchar(25),
    @valIDT datetime2(7)

  while @curI <= @stopI 
  begin
    
    set @pointStr = convert(varchar(25), @curI)

    set @valIDT = dbo.convert_KeyToDate(@curI, @checkpoint_type)

    INSERT INTO #points(pointDT, pointStr) VALUES (@valIDT, @pointStr)

    set @list = @list + @pointStr + ', ' 

    set @new_curI = dbo.KeyAddRange(@curI, @checkpoint_type, 1, @step)

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

declare @cmd varchar(max) = 'CREATE PARTITION FUNCTION ' + @pf_name + '(' + @data_type +') AS RANGE ' + @range + ' FOR VALUES (' + @list + ')'

if @cmd is null
begin
  raiserror('Failed to build CREATE PARTITION FUNCTION statement!', 16, 0)
  return
end

if @print_only = 1
  print @cmd
else
  exec (@cmd)

if not exists(select 1 from dba.PF_Function where pf_name = @pf_name) and @print_only = 0
begin
  INSERT INTO dba.PF_Function (pf_name, checkpoint_type, step) values (@pf_name, @checkpoint_type, @step)
end

--
-- PS
--
if @ps_name is not null --let's create PS
begin
  UPDATE t
  SET t.FGName = f.FGName
  FROM #points t
    join dba.PF_FileGroup_Config f
      on  (@range = 'RIGHT' and t.pointDT >= DateFrom and t.pointDT <  DateTo)
       or (@range = 'LEFT'  and t.pointDT >  DateFrom and t.pointDT <= DateTo)

  declare @notAssignedPoint datetime2(7)
  set @notAssignedPoint = (select top 1 pointDT from #points where FGName is null)

  if @notAssignedPoint is not null
  begin
    set @msg = 'Can''t find filegroup for point ' + CONVERT(varchar(32), @notAssignedPoint, 121) + ' !' 
    raiserror(@msg, 16, 0)
    return
  end

  declare @ExtraFG sysname = 
    case @range
      when 'RIGHT' then 'FG_HIST'
      when 'LEFT' then 'FG_HEAD'
    end

  set @list = ''

  if @range = 'RIGHT' 
    set @list = '[' + @ExtraFG + '], '

  SELECT @list = @list + '[' + t.FGName + '], '
  FROM #points t
  ORDER BY id

  if @range = 'LEFT' 
    set @list = @list + '[' + @ExtraFG + '], '

  if len(@list) > 2
    set @list = substring(@list, 1, len(@list) - 1)

  set @cmd = 'CREATE PARTITION SCHEME ' + @ps_name + ' AS PARTITION ' + @pf_name + ' TO (' + @list + ')'

  if @cmd is null
  begin
    raiserror('Failed to build CREATE PARTITION SCHEME statement!', 16, 0)
    return
  end

  if @print_only = 1
    print @cmd
  else
    exec (@cmd)
end --create PS
go
