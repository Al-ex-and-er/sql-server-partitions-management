--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
--DROP PROCEDURE testSSPM.Test_Create_PF_int
CREATE or alter PROCEDURE testSSPM.Test_Create_PF_int
as
set nocount on

exec sspm.CreatePF
  @PFName   = 'Test_Create_PF_int',
  @Start    = 10,
  @Stop     = 20,
  @Step     = 10,
  @DataType = 'int',
  @PSName   = 'Test_Create_PS_int',
  @PSAllTo  = 'PRIMARY'

if not exists (select 1 from sys.partition_schemes where name = 'Test_Create_PS_int')
begin
  exec tSQLt.Fail 'PS has not been created!'
  return
end

if not exists (select 1 from sys.partition_functions where name = 'Test_Create_PF_int')
begin
  exec tSQLt.Fail 'PF has not been created!'
  return
end

declare 
  @function_id int = (select function_id from sys.partition_functions where name = 'Test_Create_PF_int'),
  @cnt int

select @cnt = count(*)
from sys.partition_range_values
where function_id = @function_id 

if 2 <> @cnt
begin
  exec tSQLt.Fail 'PF expected to have 2 boundary values!'
  return
end

select @cnt = count(*)
from sys.partition_range_values
where function_id = @function_id 
  and value in (10, 20)

if 2 <> @cnt
begin
  exec tSQLt.Fail 'Wrong boundary values in the PF definition!'
  return
end

/*
DROP PARTITION SCHEME Test_Create_PS_int
DROP PARTITION FUNCTION Test_Create_PF_int
*/
go
