--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
/*
  When we switch out partition, we can handle only partition as a whole.
  When we provide a range of values to switch out, we have to find out first and last whole partitions inside of this range.
  This function returns ID of the LAST whole partition that can be switched out, @value ends the range.
  Function answers question, what is the last full partition that includes @value or comes before the value?
  
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
      5     -- this value is in the middle of PID #1. There is no previous partition so result is NULL
     10     #1 this value starts PID #2 so we can use previous partition, PID #1
     15     #1 this value is in the middle of PID # 2, we can use previous one, PID #1
     20     #2 this value starts PID #3, we can use PID #2 as a last full partition
     25     #2 this value is in the middle of the rightmost partition. Result is a previous partition
   NULL     #3 the only way to access last partition

  For the LEFT function, ranges are like this

  pid range
  --- -----
   #1 (-inf, 10]
   #2 (10, 20]
   #3 (20, +inf]

  value result explanation
  ----- ------ --------------
      5     -- this value is in the middle of PID #1. There is no previous partition so result is NULL
     10     #1 this value ends PID #1 so we can use PID #1 
     15     #1 this value is in the middle of PID # 2. Since it is in the middle, we should use previous one, PID #1
     20     #2 this value ends PID #2, we can use it
     25     #2 this value is in the middle of the rightmost partition. Result is a previous partition
   NULL     #3 the only way to access last partition

  As we can see, results for LEFT and RIGHT functions are the same

*/
CREATE OR ALTER FUNCTION sspm.GetStopPID (@PFName sysname, @value sql_variant)
returns int
as
begin
return
(
  SELECT 
    pid =  
      case 
        when @value is null then pf.fanout -- null means 'to the the rightmost partition'
        when v.pid = 1 then null --We can't use first partition as a last partition
        --if values is between two points, we should take previous one
        --when @value != value then v.pid - 1
        --if value is at the point and function is LEFT, we can stop at this partition
        --when @value = value and pf.boundary_value_on_right = 0 then v.pid - 1
        --LEFT functions use the same numbers as RIGHT ones
        else v.pid - 1
      end  
  FROM sys.partition_functions pf
    outer apply --find a boundary at @value or to the left from @value
    ( SELECT top 1 v.boundary_id, v.value   
      FROM sys.partition_range_values v
      WHERE v.function_id = pf.function_id
        and v.value <= @value
      ORDER BY v.boundary_id desc  
    ) b
    outer apply (select pid = isnull(b.boundary_id, 0) + 1) v --convert boundary_id to RIGHT partition ID
  WHERE pf.name = @PFName
)
end
go
