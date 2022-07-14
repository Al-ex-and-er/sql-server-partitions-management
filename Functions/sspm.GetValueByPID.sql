--
-- Copyright (c) 2022 Alexander (Oleksandr) Sinitsyn
--
/*
  Returns a known value for a partition id of @PFName
  for a RIGHT partitioning function, for example, we can't return a value for partition # 1
  partition #1 is [-inf, N) so N is not included. Everything that is less than N is in partition #1 
  but from the function definition we don't know what value it could be.
  for the same function, a value for partition #2 is N, because N is the first value of a range.

  PID      1      2      3
  points -----10-----20----->

  If function is RIGHT:

  pid range       result
  --- -----       ------
   #1 [-inf, 10)  NULL, we don't know a value from the function definition
   #2 [10, 20)    10
   #3 [20, +inf)  20
   #4             NULL, no such partition
  
  If function is LEFT:

  pid range       result
  --- -----       ------
   #1 (-inf, 10]  10
   #2 (10, 20]    20
   #3 (20, +inf]  NULL, we don't know a value from the function definition
   #4             NULL, no such partition

*/
CREATE OR ALTER FUNCTION sspm.GetValueByPID (@PFName sysname, @PID int)
returns sql_variant
as
begin
return
(
  SELECT v.value
  FROM sys.partition_functions pf
    join sys.partition_range_values v
      on v.function_id = pf.function_id
  WHERE pf.name = @PFName
    and v.boundary_id = 
      case 
        when pf.boundary_value_on_right = 0 then @PID
        else @PID - 1 
      end
)
end
go
