/*
The main goal of sspm.CopyTableDefinition is to create a table that can be a target of switch partition.
Because of this it is not a perfect table schema replication procedure.
Procedure is long and ugly, I'd love to see this task solved as part of SQL Server standard tooling.

  @SourceTable - one-point name, schema.table_name. two-point name is fine if DB name is the same as current DB.
  @TargetTable - same as @SourceTable
  @UsePS bit, if 0 creates @TargetTable as non-partitioned table, if 1 uses PS of source table (or @TargetPSName if specified)
  @SkipIndexes
    0   - create all indexes
    1   - omit all non-clustered indexes
    2   - omit all columnstore clustered indexes
    255 - omit all indexes
  @debug
    1 - don't do any actions, print-only
    0 - do all actions, don't print

How to move table on a new partition schema
  @UsePS = 1,
  @TargetPSName = 'New PS name',

How to create new table on the same PS
  @UsePS = 1
  @TargetPSName = null --default
*/

ALTER PROCEDURE sspm.CopyTableDefinition
  @SourceTable sysname,
  @TargetTable sysname,
  @UsePS bit = 0,
  @TargetPSName sysname = null,
  @SkipIndexes tinyint = 0,
  @DropTargetIfExists bit = 0,
  @debug bit = 0
as
set nocount on
set ansi_padding on

if ltrim(rtrim(isnull(@SourceTable, ''))) = ''
begin
  raiserror('@SourceTable can''t be NULL or empty', 16, 1)
  return
end

if ltrim(rtrim(isnull(@TargetTable, ''))) = ''
begin
  raiserror('@TargetTable can''t be NULL or empty', 16, 1)
  return
end

declare
  @dbName nvarchar(128) = DB_NAME(),
  @tmpObject sysname = @SourceTable,
  @tmpDB nvarchar(128), @tmpSchema sysname, @tmpTable sysname, @tmpSrv sysname

select
  @tmpTable  = parsename(@tmpObject, 1),
  @tmpSchema = isnull(parsename(@tmpObject, 2), N'dbo'),
  @tmpDB     = isnull(parsename(@tmpObject, 3), db_name()),
  @tmpSrv    = parsename(@tmpObject, 4)

if @tmpSrv is not null or @tmpDB <> @dbName
begin
  raiserror('@SourceTable is not valid', 16, 1)
  return
end

set @SourceTable = quotename(@tmpSchema) + '.' + quotename(@tmpTable)
set @tmpObject = @TargetTable

select
  @tmpTable  = parsename(@tmpObject, 1),
  @tmpSchema = isnull(parsename(@tmpObject, 2), N'dbo'),
  @tmpDB     = isnull(parsename(@tmpObject, 3), db_name()),
  @tmpSrv    = parsename(@tmpObject, 4)

if @tmpSrv is not null or @tmpDB <> @dbName
begin
  raiserror('@TargetTable is not valid', 16, 1)
  return
end

set @TargetTable = quotename(@tmpSchema) + '.' + quotename(@tmpTable)

if @debug = 1
  print 'source table: ' + @SourceTable + ' target table: ' + @TargetTable

if @UsePS = 0 and @TargetPSName is not null
begin
  raiserror('@TargetPSName should not be specified if @UsePS = 0', 16, 1)
  return
end

if @TargetPSName is not null
  and not exists(select 1 from sys.partition_schemes where name = @TargetPSName)
begin
  raiserror('Can''t find @TargetPSName', 16, 1)
  return
end

if @SkipIndexes not in(0, 1, 2, 255)
begin
  raiserror('Not supported value in @SkipIndexes', 16, 1)
  return
end

declare @ObjectId int = object_id(@SourceTable, 'U')

if @ObjectId is null
begin
  raiserror('Source table can''t be found', 16, 1)
  return
end

declare
  @SkipNCIndexes  bit = case when @SkipIndexes = 1   then 1 else 0 end,
  @SkipCCIndexes  bit = case when @SkipIndexes = 2   then 1 else 0 end,
  @SkipAllIndexes bit = case when @SkipIndexes = 255 then 1 else 0 end

declare
  @Compression varchar(20),
  @SourcePSName sysname,
  @PartColumn sysname,
  @FileGroupName sysname

-- Object Info
SELECT
  @Compression = p.data_compression_desc,
  @PartColumn = quotename(col.PartColumn)
FROM sys.indexes i with (nolock)
  join sys.partitions p with (nolock)
    on p.[object_id] = i.[object_id]
      and p.index_id = i.index_id
  outer apply
  (
    select PartColumn = c.name
    from sys.index_columns ic with(nolock)
     join sys.columns c with(nolock)
        on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
    where
      ic.index_id = i.index_id
      and ic.[object_id] = i.[object_id]
      and ic.partition_ordinal > 0
  )col
WHERE
  i.index_id in (0, 1)
  and p.partition_number = 1
  and i.[object_id] = @ObjectId

if @debug = 1
  print 'data compression: ' + isnull(@Compression, 'NONE') + ' partitioning column: ' + isnull(@PartColumn, '<none>')

SELECT
  @FileGroupName = quotename(fg.name),
  @SourcePSName = quotename(ps.name)
FROM sys.indexes i with (nolock)
  inner join sys.partitions p with (nolock)
    on i.[object_id] = p.[object_id]
      and i.index_id = p.index_id
  left join sys.partition_schemes ps with (nolock)
    on i.data_space_id = ps.data_space_id
  left join sys.destination_data_spaces dds with (nolock)
    on ps.data_space_id = dds.partition_scheme_id
      and p.partition_number = dds.destination_id
  inner join sys.filegroups fg with (nolock)
    on coalesce(dds.data_space_id, i.data_space_id) = fg.data_space_id
 WHERE
  i.[object_id] = @ObjectId

if @debug = 1
  print 'source file group: ' + isnull(@FileGroupName, '-') + ' source partitioning schema: ' + isnull(@SourcePSName, '-')

if @UsePS = 1 and @SourcePSName is null and @TargetPSName is null
begin
  print '@UsePS = 1 but source table is not partitioned and @TargetPSName is not specified, @UsePS was re-set to 0'
  set @UsePS = 0
end

declare @PSName sysname = @SourcePSName

if @UsePS = 1 and @TargetPSName is not null
  set @PSName = @TargetPSName

if @debug = 1
  print 'target partitioning schema: ' + @PSName

declare @cmd nvarchar(max)
declare @ddl table(id int identity(1,1) not null, query nvarchar(max) not null)

if object_id(@TargetTable) is not null
begin
  if @DropTargetIfExists = 1
  begin
    set @cmd = N'if (object_id(''' + @TargetTable + ''') is not null) DROP TABLE ' + @TargetTable
    INSERT INTO @ddl(query) VALUES (@cmd)
    if @debug = 1 print @cmd
  end
  else
  begin
    if @debug = 0
    begin
      raiserror('@TargetTable exists, please use @DropTargetIfExists = 1 to drop it', 16, 1)
      return
    end
    else
      print '@TargetTable exists, please use @DropTargetIfExists = 1 to drop it'
  end
end

declare @PhysicalLocation varchar(1024) = ''

if @UsePS = 0
begin
  if @FileGroupName is not null
    set @PhysicalLocation = ' ON ' + @FileGroupName
end
else
  set @PhysicalLocation = ' ON ' + @PSName + '(' + @PartColumn + ') '

declare @TableOptions varchar(2048) = ''

if @Compression not in ('NONE', 'COLUMNSTORE', 'COLUMNSTORE_ARCHIVE')
  set @TableOptions = ' WITH (DATA_COMPRESSION = ' + @Compression + ')'

declare
  @createTableHeader nvarchar(1024) = N'CREATE TABLE '+ @TargetTable + N'(',
  @createTableColumns nvarchar(max) = N'',
  @createTableFooter nvarchar(1024) = N')' + @PhysicalLocation + @TableOptions

;with column_def as
(
  SELECT
    column_name = c.name,
    column_ordinal = row_number() over(partition by c.object_id order by c.column_id asc),
    c.is_nullable,
    data_type = st.name,
    max_length =
      case
        when c.max_length = -1 then 'max'
        when st.name in (N'nchar', N'nvarchar', N'ntext') then cast(c.max_length/2 as varchar(16))
        else cast(c.max_length as varchar(16))
      end,
    c.precision,
    c.scale,
    c.collation_name,
    cc.definition,
    cc.is_persisted,
    is_user_type = case when c.system_type_id <> c.user_type_id then 1 else 0 end
  FROM sys.columns c
    join sys.types st
      ON st.user_type_id = c.user_type_id
    left join sys.identity_columns ic
      on c.object_id = ic.object_id AND c.column_id = ic.column_id
    left join sys.computed_columns cc
      on c.object_id = cc.object_id and c.column_id = cc.column_id
  WHERE c.object_id = @ObjectId
)
select @createTableColumns = @createTableColumns +
  N'[' + column_name + N'] '
  + case
      when definition is null and is_user_type = 0
        then --non-calculated column
          data_type
          + case when collation_name is not null and data_type not in (N'text', N'ntext') then N'(' + max_length + N') collate ' + collation_name else N'' end
          + case when data_type in (N'decimal', N'numeric') then N'(' + cast(precision as varchar(16)) + N', ' + cast(scale as varchar(16)) + N')' else N'' end
          + case when data_type in (N'float') then N'(' + cast(precision as varchar(16)) + N')' else N'' end
          + case when data_type in (N'datetime2', N'datetimeoffset') then N'(' + cast(scale as varchar(16)) + N')' else N'' end
          + case when data_type in (N'varbinary', N'binary') then N'(' + max_length + N')' else N'' end
          + case is_nullable when 0 then N' not null' else N' null' end
      when is_user_type = 1
        then data_type
          + case is_nullable when 0 then N' not null' else N' null' end
    else --calculated column
      N' as ' + definition
      + case is_persisted when 1 then N' PERSISTED' else N'' end
    end
  + N', '
from column_def
order by column_ordinal

if len(@createTableColumns) > 1
  set @createTableColumns = substring(@createTableColumns, 1, len(@createTableColumns)-1)

-- Create the table
set @cmd = @createTableHeader + @createTableColumns + @createTableFooter

INSERT INTO @ddl(query) VALUES (@cmd)

if @debug = 1 print @cmd

if @SkipAllIndexes = 0
begin --Build indexes
  declare
    @IndexId int,
    @IndexName nvarchar(255),
    @IsUnique bit,
    @IsUniqueConstraint bit,
    @IsPrimaryKey bit,
    @IsIncremental bit,
    @FilterDefinition nvarchar(max),
    @IndexType int,
    @count int = 0,
    @Unique nvarchar(255),
    @KeyColumns nvarchar(max),
    @IncludedColumns nvarchar(max),
    @DataCompressionDesc nvarchar(60),
    @Options nvarchar(2048)

  declare indexcursor cursor for
    SELECT
      i.index_id,
      i.name,
      i.is_unique,
      i.is_unique_constraint,
      i.is_primary_key,
      i.filter_definition,
      i.[type],
      p.data_compression_desc,
      quotename(fg.name),
      quotename(ps.name),
      st.is_incremental
    FROM sys.indexes i
      join sys.partitions p with (nolock)
        on p.[object_id] = i.[object_id]
        and p.index_id = i.index_id
      left join sys.partition_schemes ps with (nolock)
        on i.data_space_id = ps.data_space_id
      left join sys.destination_data_spaces dds with (nolock)
        on ps.data_space_id = dds.partition_scheme_id
        and p.partition_number = dds.destination_id
      left join sys.filegroups fg with (nolock)
        on coalesce(dds.data_space_id, i.data_space_id) = fg.data_space_id
      join sys.stats st with (nolock)
        on st.[object_id] = i.[object_id] AND st.name = i.name
    WHERE i.[object_id] = @ObjectId
      and p.partition_number = 1
    ORDER BY i.index_id

  open indexcursor

  fetch next from indexcursor into @IndexId, @IndexName, @IsUnique, @IsUniqueConstraint, @IsPrimaryKey, @FilterDefinition, @IndexType, @DataCompressionDesc, @FileGroupName, @PSName, @IsIncremental

  while @@fetch_status = 0
  begin

    --if @debug = 1
    --  print
    --      '@IndexId = ' + isnull(cast(@IndexId as varchar(16)), 'NULL') + char(10)
    --    + '@IndexName = '+ isnull(@IndexName, 'NULL') + char(10)
    --    + '@IsUnique = '+ isnull(cast(@IsUnique as char(1)), 'NULL') + char(10)
    --    + '@IsUniqueConstraint = '+ isnull(cast(@IsUniqueConstraint as char(1)), 'NULL') + char(10)
    --    + '@IsPrimaryKey = '+ isnull(cast(@IsPrimaryKey as char(1)), 'NULL') + char(10)
    --    + '@FilterDefinition = '+ isnull(@FilterDefinition, 'NULL') + char(10)
    --    + '@IndexType = '+ isnull(cast(@IndexType as varchar(10)), 'NULL') + char(10)
    --    + '@DataCompressionDesc = '+ isnull(@DataCompressionDesc, 'NULL') + char(10)
    --    + '@FileGroupName = '+ isnull(@FileGroupName, 'NULL') + char(10)
    --    + '@PSName = '+ isnull(@PSName, 'NULL') + char(10)

    set @count += 1

    /*
    if @IndexId = 0 --heap
    begin
      if @UsePS = 0 --move table to target file group
      begin
        set @cmd = N'ALTER TABLE ' + @TargetTable + N' ADD CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_') +  N' UNIQUE CLUSTERED(' + @PartColumn + N')' + @PhysicalLocation
        if @debug = 1 print @cmd
        exec (@cmd)
        if @@error > 0 goto error_handler

        set @cmd = N'ALTER TABLE ' + @TargetTable + N' DROP CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_')
        if @debug = 1 print @cmd
        exec (@cmd)
        if @@error > 0 goto error_handler
      end
      else if @UsePS = 1
      begin --it is only way to put heap on PS
        set @cmd = N'ALTER TABLE ' + @TargetTable + N' ADD CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_') +  N' UNIQUE CLUSTERED(' + @PartColumn + N')'
        if @debug = 1 print @cmd
        exec (@cmd)
        if @@error > 0 goto error_handler

        set @cmd = N'ALTER TABLE ' + @TargetTable + N' DROP CONSTRAINT UQ_' + replace(@TargetTable, N'.', N'_') +  N' WITH(MOVE TO ' + @PSName + N'(' + @PartColumn + N'))'
        if @debug = 1 print @cmd
        exec (@cmd)
        if @@error > 0 goto error_handler
      end

      if @DataCompressionDesc != 'NONE'
      begin
        set @cmd = N'ALTER TABLE ' + @TargetTable + N' REBUILD WITH (DATA_COMPRESSION = ' + @DataCompressionDesc + N')'
        if @debug = 1 print @cmd
        exec (@cmd)
        if @@error > 0 goto error_handler
      end

    end --@IndexId = 0 --heap
    else
    */

    if @IndexType = 5 and @SkipCCIndexes = 0 --Columnstore Clustered
    begin
      set @cmd = N'CREATE CLUSTERED COLUMNSTORE INDEX [' + @IndexName + N'] ON ' + @TargetTable

      INSERT INTO @ddl(query) VALUES (@cmd)

      if @debug = 1 print @cmd
    end
    else if @IndexType = 6 --@SkipNCCIndexes = 0
    begin
      set @IncludedColumns = N''

      SELECT @IncludedColumns = @IncludedColumns + N'[' + c.name + N'], '
      FROM sys.index_columns ic
        join sys.columns c
          on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
      WHERE index_id = @IndexId
        and ic.[object_id] = @ObjectId
        and key_ordinal = 0
      ORDER BY index_column_id

      if len(@IncludedColumns) > 0
        set @IncludedColumns = left(@IncludedColumns, len(@IncludedColumns) - 1)

      if @FilterDefinition is null
        set @FilterDefinition = N''
      else
        set @FilterDefinition = N'WHERE ' + @FilterDefinition + N' '

      set @Options = N''

      if @DataCompressionDesc != N'NONE'
        set @Options = N'DATA_COMPRESSION = ' + @DataCompressionDesc

      if @Options > N''
        set @Options = N' WITH (' + @Options + N')'

        set @cmd = N'CREATE NONCLUSTERED COLUMNSTORE INDEX [' + @IndexName + N'] ON ' 
          + @TargetTable + N' (' + @IncludedColumns + N')'
          + @FilterDefinition + @Options + @PhysicalLocation

        INSERT INTO @ddl(query) VALUES (@cmd)

        if @debug = 1 print @cmd

    end
    else --'normal' indexes
    begin
      set @Unique = case when @IsUnique = 1 then N' UNIQUE ' else N'' end
      set @KeyColumns = N''
      set @IncludedColumns = N''

      SELECT @KeyColumns = @KeyColumns + N'[' + c.name + N'] ' + CASE WHEN is_descending_key = 1 THEN N'DESC' ELSE N'ASC' END + N','
      FROM sys.index_columns ic
        join sys.columns c
          on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
      WHERE index_id = @IndexId
        and ic.[object_id] = @ObjectId
        and ic.key_ordinal > 0
      ORDER BY ic.key_ordinal

      SELECT @IncludedColumns = @IncludedColumns + N'[' + c.name + N'],'
      FROM sys.index_columns ic
        join sys.columns c
          on c.[object_id] = ic.[object_id]
          and c.column_id = ic.column_id
      WHERE index_id = @IndexId
        and ic.[object_id] = @ObjectId
        and key_ordinal = 0
      ORDER BY index_column_id

      if len(@KeyColumns) > 0
        set @KeyColumns = left(@KeyColumns, len(@KeyColumns) - 1)

      if len(@IncludedColumns) > 0
        set @IncludedColumns = N' INCLUDE (' + LEFT(@IncludedColumns, LEN(@IncludedColumns) - 1) + N')'

      if @FilterDefinition is null
        set @FilterDefinition = N''
      else
        set @FilterDefinition = N'WHERE ' + @FilterDefinition + N' '

      set @Options = N''

      if @DataCompressionDesc != N'NONE'
        set @Options = N'DATA_COMPRESSION = ' + @DataCompressionDesc

      if @IsIncremental = 1 and @UsePS = 1
      begin
        if @Options > N''
          set @Options += N', '

        set @Options += N'STATISTICS_INCREMENTAL = ON'
      end

      if @Options > N''
        set @Options = N' WITH (' + @Options + N') '

      if @IsPrimaryKey = 1
      begin
        declare @PKName sysname = N'PK_' + replace(replace(replace(@TargetTable, N'.', N'_'), N'[', N''), N']', N'')

        set @cmd = N'ALTER TABLE ' + @TargetTable
          + N' ADD CONSTRAINT ' + @PKName
          + N' PRIMARY KEY (' + @KeyColumns + N')'
          + @Options + @PhysicalLocation

        INSERT INTO @ddl(query) VALUES (@cmd)

        if @debug = 1 print @cmd
      end
      else if @SkipNCIndexes = 0 or (@SkipNCIndexes = 1 and @IndexType = 1) -- 1 = Clustered
      begin
        set @cmd = N'CREATE '
          + case @IsUnique  when 1 then N'UNIQUE ' else N'' end
          + case @IndexType when 2 then N'NONCLUSTERED ' else N'CLUSTERED ' end
          + N'INDEX [' + @IndexName + N'] ON ' + @TargetTable + N' (' + @KeyColumns + N')'
          + @IncludedColumns + @FilterDefinition + @Options + @PhysicalLocation

        INSERT INTO @ddl(query) VALUES (@cmd)

        if @debug = 1 print @cmd
      end -- /@SkipNCIndexes check
    end -- /'normal' indexes

    fetch next from indexcursor into @IndexId, @IndexName, @IsUnique, @IsUniqueConstraint, @IsPrimaryKey, @FilterDefinition, @IndexType, @DataCompressionDesc, @FileGroupName, @PSName, @IsIncremental

  end --/index list

  close indexcursor
  deallocate indexcursor

end --@SkipAllIndexes = 0

if @debug = 0 --run DDL
begin try
  declare
    @id int = 1,
    @cnt int = (select COUNT(*) from @ddl)

  BEGIN TRAN

  while @id <= @cnt
  begin
    select @cmd = query from @ddl where id = @id

    exec(@cmd)

    set @id += 1
  end

  COMMIT TRAN
end try
begin catch
  if xact_state() <> 0
    ROLLBACK TRANSACTION

  ;throw;
end catch
--/run DDL

-- Define the check constraint
/*
if @LoadDay > 0
begin
  set @cmd = N'ALTER TABLE ' + @TargetTable + N' ADD Constraint CK_' + @TargetTable + N'_' + @PartColumn + N' CHECK (' +  @PartColumn + N' = ' + Convert(char(8), @LoadDay) + N')'
  if @debug = 1 print @cmd
  exec (@cmd)
  if @@error > 0 goto error_handler
end
*/

GO
