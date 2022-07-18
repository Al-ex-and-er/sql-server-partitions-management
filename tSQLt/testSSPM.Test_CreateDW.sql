--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
--DROP PROCEDURE testSSPM.Test_CreateDW
CREATE or alter PROCEDURE testSSPM.Test_CreateDW
as
set nocount on

exec sspm.CreateDW
  @PFName   = 'PF_Test_CreateDW',
  @Start     = 20200101,
  @Stop      = 20210101,
  @Step      = '1 day',
  @KeyType   = 'DateKey',
  @PSName   = 'PS_Test_CreateDW',
  @PSAllTo  = 'PRIMARY'

if not exists (select 1 from sys.partition_schemes where name = 'PS_Test_CreateDW')
begin
  exec tSQLt.Fail 'PS has not been created!'
  return
end

if not exists (select 1 from sys.partition_functions where name = 'PF_Test_CreateDW')
begin
  exec tSQLt.Fail 'PF has not been created!'
  return
end

declare 
  @function_id int = (select function_id from sys.partition_functions where name = 'PF_Test_CreateDW'),
  @cnt int

select @cnt = count(*)
from sys.partition_range_values
where function_id = @function_id 

if 367 <> @cnt
begin
  exec tSQLt.Fail 'PF expected to have 367 boundary values!'
  return
end

select @cnt = count(*)
from sys.partition_range_values
where function_id = @function_id 
  and value in (20200101, 20210101)

if 2 <> @cnt
begin
  exec tSQLt.Fail 'Wrong boundary values in the PF definition!'
  return
end

/*
DROP PARTITION SCHEME   PS_Test_CreateDW
DROP PARTITION FUNCTION PF_Test_CreateDW
*/
go
