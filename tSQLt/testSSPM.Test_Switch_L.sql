CREATE OR ALTER PROCEDURE testSSPM.Test_Switch_L
as
set nocount on

CREATE PARTITION FUNCTION PF_Switch_L (int) AS RANGE LEFT FOR VALUES (10, 20)
CREATE PARTITION SCHEME PS_Switch_L AS PARTITION PF_Switch_L ALL TO ([PRIMARY])

CREATE TABLE dbo.Test_Switch_L(c1 int not null, c2 varchar(10)) on PS_Switch_L(c1) 

INSERT INTO dbo.Test_Switch_L VALUES
( 5, 'A'),
(10, 'B'),
(15, 'C'),
(20, 'D'),
(25, 'E')

CREATE TABLE dbo.Test_Switch_Temp_L(c1 int not null, c2 varchar(10)) on PS_Switch_L(c1) 

CREATE TABLE #expected
(
  id char(4) not null primary key,
  src varchar(100) not null,
  dst varchar(100) not null,
  v1 int null, --from
  v2 int null, --to
  pid1 int null,
  pid2 int null,
  ins int not null,
  vals varchar(20) not null
)

INSERT INTO #expected
(    id,                 src,                      dst,   v1,   v2, pid1,  pid2, ins,            vals) VALUES
('__05', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L', NULL,    5, NULL, NULL,    0, ''             ),
('__10', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L', NULL,   10,    1,    1,    2, '5,10'         ),
('__15', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L', NULL,   15,    1,    1,    2, '5,10'         ),
('__20', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L', NULL,   20,    1,    2,    4, '5,10,15,20'   ),
('__25', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L', NULL,   25,    1,    2,    4, '5,10,15,20'   ),
('____', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L', NULL, NULL,    1,    3,    5, '5,10,15,20,25'),
('0505', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',    5,    5, NULL, NULL,    0, ''             ),
('0510', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',    5,   10, NULL, NULL,    0, ''             ),
('0515', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',    5,   15, NULL, NULL,    0, ''             ),
('0520', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',    5,   20,    2,    2,    2, '15,20'        ),
('0525', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',    5,   25,    2,    2,    2, '15,20'        ),
('05__', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',    5, NULL,    2,    3,    3, '15,20,25'     ),
('1010', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   10,   10, NULL, NULL,    0, ''             ),
('1015', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   10,   15, NULL, NULL,    0, ''             ),
('1020', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   10,   20,    2,    2,    2, '15,20'        ),
('1025', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   10,   25,    2,    2,    2, '15,20'        ),
('10__', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   10, NULL,    2,    3,    3, '15,20,25'     ),
('1515', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   15,   15, NULL, NULL,    0, ''             ),
('1520', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   15,   20, NULL, NULL,    0, ''             ),
('1525', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   15,   25, NULL, NULL,    0, ''             ),
('15__', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   15, NULL,    3,    3,    1, '25'           ),
('2020', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   20,   20, NULL, NULL,    0, ''             ),
('2025', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   20,   25, NULL, NULL,    0, ''             ),
('20__', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   20, NULL,    3,    3,    1, '25'           ),
('2525', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   25,   25, NULL, NULL,    0, ''             ),
('25__', 'dbo.Test_Switch_L', 'dbo.Test_Switch_Temp_L',   25, NULL, NULL, NULL,    0, ''             )

declare test_cursor cursor
read_only
FOR 
SELECT id, src, dst, v1, v2, pid1, pid2, ins, vals
FROM #expected

declare 
  @id char(4),
  @src varchar(100),
  @dst varchar(100),
  @v1 int,
  @v2 int,
  @pid1 int,
  @pid2 int,
  @ins int,
  @vals varchar(20)

declare 
  @RowsInserted bigint, 
  @RowsDeleted bigint,
  @msg varchar(200),
  @expected_count int,
  @actual_count int,
  @matching_count int,
  @err int

declare @expected_values table (v int)

open test_cursor

fetch next from test_cursor into @id, @src, @dst, @v1, @v2, @pid1, @pid2, @ins, @vals
while @@fetch_status <> -1
begin
	if @@fetch_status <> -2
	begin
    print ''
    set @msg = concat(@id, ' ', @src, ' -> ', @dst, ', from ', isnull(cast(@v1 as varchar(16)), 'NULL'), ' to ', isnull(cast(@v2 as varchar(16)), 'NULL'))
    raiserror(@msg, 0, 0) with nowait
    --
    -- Switch data in the forward direction, dbo.Test_Switch_L -> dbo.Test_Switch_Temp_L
    --
    begin try
      set @err = 0

      exec sspm.Switch
        @SourceTable = @src,
        @TargetTable = @dst,
        @From = @v1,
        @To = @v2,
        @SkipCount = 0,
        @RowsInserted = @RowsInserted out,
        @RowsDeleted = @RowsDeleted out,
        @Debug = 1
    end try
    begin catch
      set @err = 1
      ;throw;
    end catch

    if @err = 1
    begin
      fetch next from test_cursor into @id, @src, @dst, @v1, @v2, @pid1, @pid2, @ins, @vals
      continue
    end

    if @RowsInserted <> @ins
    begin
      set @msg = '@RowsInserted = ' + cast(@RowsInserted as varchar(10)) + ', expected ' + cast(@ins as varchar(10)) + ' rows!'
      exec tSQLt.Fail @msg
    end

    if @RowsInserted > 0
    begin
      DELETE FROM @expected_values

      INSERT INTO @expected_values(v)
      SELECT v = value FROM string_split(@vals, ',');

      set @expected_count = @@rowcount

      select @actual_count = count(*) from dbo.Test_Switch_Temp_L

      if @expected_count <> @actual_count
      begin
        set @msg = 'Expected number of rows in the target table is ' + cast(@expected_count as varchar(10)) + ', actual number is ' + cast(@actual_count as varchar(10)) + ' rows!'
        exec tSQLt.Fail @msg
      end

      select @matching_count = count(*)
      from @expected_values e
        join dbo.Test_Switch_Temp_L t
         on e.v = t.c1

      if @matching_count <> @expected_count
      begin
        set @msg = 'Expected number of matching rows is ' + cast(@expected_count as varchar(10)) + ', actual number is ' + cast(@matching_count as varchar(10)) + ' rows!'
        exec tSQLt.Fail @msg
      end

      select @matching_count = count(*)
      from @expected_values e
        join dbo.Test_Switch_L t
         on e.v = t.c1

      if @matching_count <> 0
      begin
        set @msg = 'This number of values shouldn''t be present in the source table, ' + cast(@matching_count as varchar(10)) + '!'
        exec tSQLt.Fail @msg
      end
    end --/if @RowsInserted > 0


    if @RowsInserted = 0 --don't switch back if no data to switch
    begin
      fetch next from test_cursor into @id, @src, @dst, @v1, @v2, @pid1, @pid2, @ins, @vals
      continue
    end
    --
    -- Switch data in the reverse direction, dbo.Test_Switch_Temp_L -> dbo.Test_Switch_L
    --
    print ''
    set @msg = concat(@id, ' ', @dst, ' -> ', @src, ', from ', isnull(cast(@v1 as varchar(16)), 'NULL'), ' to ', isnull(cast(@v2 as varchar(16)), 'NULL'))
    raiserror(@msg, 0, 0) with nowait

    exec sspm.Switch
      @SourceTable = @dst,
      @TargetTable = @src,
      @From = @v1,
      @To = @v2,
      @SkipCount = 0,
      @RowsInserted = @RowsInserted out,
      @RowsDeleted = @RowsDeleted out,
      @Debug = 1

    if @RowsInserted <> @ins
    begin
      set @msg = '@RowsInserted = ' + cast(@RowsInserted as varchar(10)) + ', expected ' + cast(@ins as varchar(10)) + ' rows!'
      exec tSQLt.Fail @msg
    end

    if @RowsInserted > 0
    begin
      --target table should be empty
      if 0 <> (select count(*) from dbo.Test_Switch_Temp_L)
      begin
        exec tSQLt.Fail 'Target table should be empty'
      end

      if 5 <> (select count(*) from dbo.Test_Switch_L)
      begin
        exec tSQLt.Fail 'Source table should contain 5 rows'
      end

      if 75 <> (select sum(c1) from dbo.Test_Switch_L)
      begin
        exec tSQLt.Fail 'Source table should contain the same rows as before'
      end

    end --/@RowsInserted > 0
	end --/if @@fetch_status <> -2
	fetch next from test_cursor into @id, @src, @dst, @v1, @v2, @pid1, @pid2, @ins, @vals
end --/while

close test_cursor
deallocate test_cursor


DROP TABLE dbo.Test_Switch_Temp_L
DROP TABLE dbo.Test_Switch_L
DROP PARTITION SCHEME PS_Switch_L
DROP PARTITION FUNCTION PF_Switch_L
go
