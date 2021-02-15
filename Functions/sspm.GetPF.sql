--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
/*
  Returns partitioning function of a partitioned table
*/
CREATE OR ALTER FUNCTION sspm.GetPF (@TableName sysname)
returns sysname
with returns null on null input
as
begin
return
(
  SELECT top 1 pf.name
  FROM sys.indexes i with(nolock)
    join sys.partition_schemes ps with(nolock) 
      on ps.data_space_id = i.data_space_id
    join sys.partition_functions pf with(nolock) 
      on pf.function_id = ps.function_id
  WHERE  i.object_id = object_id(@TableName)
    AND i.type in(0, 1)
)
end
go
