--DROP PROCEDURE testSSPM.Test_Create_PF_datetime
CREATE or alter PROCEDURE testSSPM.Test_Create_PF_datetime
as
set nocount on

exec sspm.CreatePF
  @PFName   = 'Test_Create_PF_datetime',
  @Start    = '2020-01-01 00:00:00',
  @Stop     = '2020-01-01 06:00:00',
  @Step     = '30 minutes',
  @DataType = 'datetime',
  @PSName   = 'Test_Create_PS_datetime',
  @PSAllTo  = 'PRIMARY'

if not exists (select 1 from sys.partition_schemes where name = 'Test_Create_PS_datetime')
begin
  exec tSQLt.Fail 'PS has not been created!'
  return
end

if not exists (select 1 from sys.partition_functions where name = 'Test_Create_PF_datetime')
begin
  exec tSQLt.Fail 'PF has not been created!'
  return
end

declare 
  @function_id int = (select function_id from sys.partition_functions where name = 'Test_Create_PF_datetime'),
  @cnt int

select @cnt = count(*)
from sys.partition_range_values
where function_id = @function_id 

if 13 <> @cnt
begin
  exec tSQLt.Fail 'PF expected to have 13 boundary values!'
  return
end

select @cnt = sum(datediff(minute, '2020-01-01', cast(value as datetime)))
from sys.partition_range_values
where function_id = @function_id

if 2340 <> @cnt
begin
  exec tSQLt.Fail 'Wrong boundary values in the PF definition!'
  return
end

/*
DROP PARTITION SCHEME Test_Create_PS_datetime
DROP PARTITION FUNCTION Test_Create_PF_datetime
*/
go
