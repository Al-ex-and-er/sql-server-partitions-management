/*
This function takes partition function and range of points, and returns range of partition IDs.
It is two functions (sspm.GetStartPID, sspm.GetStopPID) combined in one.
Function returns only valid ranges so pid1 <= pid2 or return nothing.
if @value1 >= @value2, result will be NULL
*/
CREATE OR ALTER FUNCTION sspm.GetPIDRange (@PFName sysname, @value1 sql_variant, @value2 sql_variant)
RETURNS table
AS
RETURN
(
  with ranges as
  (
  SELECT 
      pid1 =  
        case 
          when @value1 is null then 1
          when v1.pid = pf.fanout and @value1 != b1.value then null
          when @value1 != b1.value or b1.value is null then v1.pid + 1
          when @value1 = b1.value and pf.boundary_value_on_right = 1 then v1.pid
          else v1.pid
        end,
      pid2 =  
        case 
          when @value2 is null then pf.fanout
          when v2.pid = 1 then null
          else v2.pid - 1
        end  
    FROM sys.partition_functions pf
      outer apply
      ( SELECT top 1 v1.boundary_id, v1.value   
        FROM sys.partition_range_values v1
        WHERE v1.function_id = pf.function_id
          and v1.value <= @value1
        ORDER BY v1.boundary_id desc  
      ) b1
      outer apply (SELECT pid = isnull(b1.boundary_id, 0) + 1) v1
      outer apply
      ( SELECT top 1 v2.boundary_id, v2.value   
        FROM sys.partition_range_values v2
        WHERE v2.function_id = pf.function_id
          and v2.value <= @value2
        ORDER BY v2.boundary_id desc  
      ) b2
      outer apply (SELECT pid = isnull(b2.boundary_id, 0) + 1) v2
    WHERE pf.name = @PFName
  )
  SELECT pid1, pid2
  FROM ranges
  WHERE isnull(pid1, 999999) <= isnull(pid2, -1)
)
go
