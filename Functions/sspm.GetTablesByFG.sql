CREATE OR ALTER FUNCTION sspm.GetTablesByFG (@FGName sysname)
RETURNS table
AS
RETURN
(
  SELECT
    i.object_id,
    table_name = sch.name + '.' + o.name,
    is_partitioned = 1
  FROM sys.data_spaces ds with(nolock) --data_space is a FG
    join sys.destination_data_spaces dds with(nolock) --link to partition_scheme_id
      on ds.data_space_id = dds.data_space_id
        and dds.destination_id = 1
    join sys.indexes i with(nolock)
      on i.data_space_id = dds.partition_scheme_id --if index is partitioned, i.data_space_id is a partition_scheme_id
    join sys.objects o with(nolock)
      on o.object_id = i.object_id
    join sys.schemas sch with(nolock)
      on sch.schema_id = o.schema_id
  WHERE ds.name = @FGName
    and ds.type = 'FG'
    and i.index_id in (0, 1)

  UNION ALL

  SELECT
    i.object_id,
    table_name = sch.name + '.' + o.name,
    is_partitioned = 0
  FROM sys.data_spaces ds with(nolock)
    join sys.indexes i with(nolock)
      on  i.data_space_id = ds.data_space_id
    join sys.objects o with(nolock)
      on o.object_id = i.object_id
    join sys.schemas sch with(nolock)
      on sch.schema_id = o.schema_id
  WHERE ds.name = @FGName
    and ds.type = 'FG'
    and i.index_id in (0, 1)
)
go
