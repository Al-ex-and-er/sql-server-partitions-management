--'1 second 2 minute 3 hour 4 day 5 week 6 month 9 year'
declare @int varchar(100) = '-1 hour'

--microsecond, millisecond, second, minute, hour, day, week, month, year, decade, century, millennium
--second, minute, hour, day, week, month, year

declare
  @second int, 
  @minute int, 
  @hour   int, 
  @day    int, 
  @week   int, 
  @month  int, 
  @year   int

;with parts as
(
  SELECT quantity = cast(quantity as int), unit
  FROM 
  (
    SELECT quantity = value, unit = lead(value) over(order by (select 1))
    FROM string_split(@int, ' ') t
  ) s
  WHERE isnumeric(quantity) = 1
)
SELECT 
  @second = [second], 
  @minute = [minute], 
  @hour   = [hour], 
  @day    = [day], 
  @week   = [week], 
  @month  = [month], 
  @year   = [year]
FROM parts p
  PIVOT (min(quantity) for unit in ([second], [minute], [hour], [day], [week], [month], [year])) t

  select
  @second,
  @minute,
  @hour  ,
  @day   ,
  @week  ,
  @month ,
  @year  