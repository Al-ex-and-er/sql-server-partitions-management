--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
/*
  When we switch out partition, we can handle only partition as a whole.
  When we provide a range of values to switch out, we have to find out first and last whole partitions inside of this range.
  This function returns ID of the FIRST whole partition that can be switched out, @value begins the range.
  Function answers question, what is the first full partition that includes @value or comes after the value?
  
  Let's consider an example, partition function with two points, 10 and 20.

  PID      1      2      3
  points -----10-----20----->
           ^  ^   ^  ^   ^
  @value   5  10  15 20  25

  If function is RIGHT, it forms these logical ranges

  pid range
  --- -----
   #1 [-inf, 10)
   #2 [10, 20)
   #3 [20, +inf)

  What partition should be first in a range?

  value result explanation
  ----- ------ --------------
   NULL     #1 the only way to access partition #1
      5     #2 this value is in the middle of PID #1. We can address only whole partition so we can start only from PID #2
     10     #2 this value _STARTS_ PID #2 so we can use PID #2 as a first partition
     15     #3 this value is in the middle of PID # 2, we can start only from PID #3
     20     #3 this value starts PID #3, we can use it
     25     -- this value is in the middle of the rightmost partition. There is no next partition so result is NULL

  For the LEFT function, ranges are like this

  pid range
  --- -----
   #1 (-inf, 10]
   #2 (10, 20]
   #3 (20, +inf]

  value result explanation
  ----- ------ --------------
   NULL     #1 the only way to access partition #1
      5     #2 this value is in the middle of PID #1. We can address only whole partition so we can start only from PID #2
     10     #2 this value _ENDS_ PID #1 and the next available partition is PID #2
     15     #3 this value is in the middle of PID # 2, we can start only from PID #3
     20     #3 this value ends PID #2, next available partition is PID #3
     25     -- this value is in the middle of the rightmost partition. There is no next partition so result is NULL

  As we can see, results for LEFT and RIGHT functions are the same

*/
CREATE OR ALTER FUNCTION sspm.GetStartPID (@PFName sysname, @value sql_variant)
returns int
as
begin
return
(
  SELECT 
    pid =  
      case 
        when @value is null then 1 -- null means 'from the leftmost partition'
        when v.pid = pf.fanout and @value != b.value then null --last partition but not the first point
        when @value != b.value or b.value is null then v.pid + 1 --in the middle of partition or first partition
        when @value = b.value and pf.boundary_value_on_right = 1 then v.pid --beginning of partition for RIGHT functions
        else v.pid --other
      end  
  FROM sys.partition_functions pf
    outer apply --find a boundary at @value or to the left from @value
    ( SELECT top 1 v.boundary_id, v.value   
      FROM sys.partition_range_values v
      WHERE v.function_id = pf.function_id
        and v.value <= @value
      ORDER BY v.boundary_id desc  
    ) b
    outer apply (SELECT pid = isnull(b.boundary_id, 0) + 1) v --convert boundary_id to RIGHT partition ID
  WHERE pf.name = @PFName
)
end
go
