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

declare 
  @type_desc nvarchar(60),
  @is_read_only bit,
  @rn int

select @type_desc = [type_desc], @is_read_only = is_read_only
from sys.filegroups 
where name = @FGName

if @type_desc is null
begin
  raiserror('No such filegroup!', 16, 0)
  return
end

if @type_desc <> 'ROWS_FILEGROUP'
begin
  raiserror('Only ROWS_FILEGROUP file groups can be defragmented!', 16, 0)
  return
end

if @is_read_only = 1 and @Debug = 0
begin
  raiserror('We can''t change a read-only file group, @Debug should be 1', 16, 0)
  return
end

if object_id('tempdb..#tables') is not null
  DROP TABLE #tables

CREATE TABLE #tables (name sysname not null)

--partitioned
INSERT INTO #tables (name)
SELECT distinct name = object_schema_name(i.object_id) + '.' + object_name(i.object_id)
FROM sys.data_spaces fg with(nolock)
  join sys.destination_data_spaces dds with(nolock)
    on dds.data_space_id = fg.data_space_id
  join sys.indexes i with(nolock)
    on i.data_space_id = dds.partition_scheme_id
WHERE fg.[name] = @FGName
  and fg.[type] = 'FG'
  and dds.destination_id = 1

set @rn = @@rowcount

--non partitioned
INSERT INTO #tables (name)
SELECT object_schema_name(i.object_id) + '.' + object_name(i.object_id)
FROM sys.indexes i
  join sys.filegroups fg
    on i.data_space_id = fg.data_space_id
WHERE fg.[name] = @FGName
  and i.[type] in (0, 1, 5)

set @rn += @@rowcount

if @rn = 0
begin
  raiserror('No tables in the filegroup!', 0, 0)
  return
end

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
