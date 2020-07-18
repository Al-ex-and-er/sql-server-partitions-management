/*
  Returns partition id of @PFName for a @value
  Returns exactly the same result as $partition.PFName(@value) so no any sense to use this function instead
*/
ALTER FUNCTION sspm.GetPID (@PFName sysname, @value sql_variant)
returns int
as
begin
return
(
  SELECT 
    pid =  
      case   
        when v.boundary_id is null then 1  
        when @value = value and pf.boundary_value_on_right = 0 then v.boundary_id  
        else v.boundary_id + 1  
      end  
  FROM sys.partition_functions pf
    outer apply
    ( SELECT top 1 v.boundary_id, v.value   
      FROM sys.partition_range_values v
      WHERE v.function_id = pf.function_id
        and v.value <= @value
      ORDER BY v.boundary_id desc  
    ) v
  WHERE pf.name = @PFName
)
end
go
