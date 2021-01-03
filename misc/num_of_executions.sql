declare 
  @d1 datetime = '2020-01-01',
  @d2 datetime = '2020-02-01'


declare
  @step_second int = 0, 
  @step_minute int = 0, 
  @step_hour   int = 12, 
  @step_day    int = 1, 
  @step_week   int = 0, 
  @step_month  int = 0, 
  @step_year   int = 0

select datediff(minute, @d1, @d2)

--44640

select @step_day * 24 * 60 + @step_hour * 60
--2160

select 44640 / 2160

--20

