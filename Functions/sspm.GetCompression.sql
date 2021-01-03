/*
  Returns compression of non-partitioned row table
*/
CREATE OR ALTER FUNCTION sspm.GetCompression (@TableName sysname)
returns nvarchar(60)
with returns null on null input
as
begin
return
(
  SELECT top 1 p.data_compression_desc
  FROM sys.indexes i with(nolock)
    join sys.partitions p with(nolock)
      on p.[object_id] = i.[object_id]
        and p.index_id = i.index_id
  WHERE i.index_id in (0, 1)
    and p.partition_number = 1
    and i.[object_id] = object_id(@TableName)
)
end
go
