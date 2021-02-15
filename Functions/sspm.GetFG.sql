--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
/*
  Returns FileGroup of non-partitioned row table
*/
CREATE OR ALTER FUNCTION sspm.GetFG (@TableName sysname)
returns sysname
with returns null on null input
as
begin
return
(
  SELECT top 1 fg.name
  FROM sys.indexes i with(nolock)
    inner join sys.filegroups fg with(nolock)
      on i.data_space_id = fg.data_space_id
   WHERE i.[object_id] = object_id(@TableName)
     and i.index_id in (0, 1)
)
end
go
