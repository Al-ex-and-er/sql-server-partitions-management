CREATE FUNCTION sspm.GetTablesByPF (@PFName sysname)
RETURNS table
AS
RETURN
(
  SELECT
    i.object_id,
    table_name = sch.name + '.' + o.name
  FROM sys.partition_functions pf with(nolock)
    join sys.partition_schemes ps with(nolock)
      on ps.function_id = pf.function_id
    join sys.indexes i with(nolock)
      on i.data_space_id = ps.data_space_id
    join sys.objects o with(nolock)
      on o.object_id = i.object_id
    join sys.schemas sch with(nolock)
      on sch.schema_id = o.schema_id
  WHERE i.index_id in (0, 1)
    and pf.name = @PFName
)
go
