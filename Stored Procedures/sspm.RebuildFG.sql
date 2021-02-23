--
-- Copyright (c) 2020 Alexander (Oleksandr) Sinitsyn
--
CREATE PROCEDURE sspm.RebuildFG
(
  @FGName sysname,
  @From   sql_variant = NULL,
  @To     sql_variant = NULL,
  @Action varchar(10) = 'AUTO',
  @SkipIfLess float = 1.0, --treshold to take any action at all,[0.0, 100.0]
  @RebuildFrom int  = 10, --treshold for avg_fragmentation_in_percent to decide what to do, REBUILD or REORGANIZE
  @Debug  bit = 0,
  @PKOnly bit = 0,
  @Online bit = 1,
  @Online_MAX_DURATION int = NULL,
  @Online_ABORT_AFTER_WAIT varchar(10) = NULL
)
as
set nocount on

if not exists(select 1 from sys.filegroups where name = @FGName)
begin
  raiserror('No such filegroup!', 16, 0)
  return
end

if object_id('tempdb..#tables') is not null
  DROP TABLE #tables

CREATE TABLE #tables (name sysname not null)

INSERT INTO #tables (name)
SELECT distinct name = object_schema_name(i.object_id) + '.' + object_name(i.object_id)
FROM sys.data_spaces fg with(nolock)
  left join sys.destination_data_spaces dds with(nolock)
  join sys.indexes i with(nolock)
    on i.data_space_id = dds.partition_scheme_id and dds.destination_id = 1
    on fg.data_space_id = dds.data_space_id and dds.destination_id = 1
WHERE fg.name = @FGName
  and fg.type = 'FG'

declare @cur_table_name sysname

declare table_names cursor for
  SELECT name
  FROM #tables
  ORDER BY name

open table_names

fetch next from table_names into @cur_table_name

while @@fetch_status = 0
begin
  raiserror(@cur_table_name, 0, 0) with nowait

  exec sspm.Rebuild
   @TableName = @cur_table_name,
   @From      = @From,
   @To        = @To,
   @IndexName = NULL,
   @Action    = @Action,
   @SkipIfLess  = @SkipIfLess,
   @RebuildFrom = @RebuildFrom,
   @Debug  = @Debug,
   @PKOnly = @PKOnly,
   @Online = @Online,
   @Online_MAX_DURATION = @Online_MAX_DURATION,
   @Online_ABORT_AFTER_WAIT = @Online_ABORT_AFTER_WAIT

 fetch next from table_names into @cur_table_name
end

close table_names
deallocate table_names
go
go
