exec sspm.CreatePF
  @PFName   = 'pf_int',
  @Start    = '2020-01-01 00:00:00',
  @Stop     = '2020-01-02 06:00:00',
  @Step     = '30 minutes',
  @DataType = 'datetime',
	@PSName   = 'ps_int',
	@PSAllTo  = 'PRIMARY',
  @PrintOnly= 1

select *
from sspm.PS_Layout

delete from sspm.PS_Layout where LayoutName = 'test_2_int_points'

INSERT INTO sspm.PS_Layout (LayoutName, PointFrom, PointTo, FGName)VALUES
('test_2_int_points', NULL, cast(10 as int), 'FG_LEFTMOST'),
('test_2_int_points', cast(10 as int), cast(20 as int), 'FG1'),
('test_2_int_points', cast(20 as int), NULL, 'FG2')


exec sspm.CreatePF
  @PFName   = 'pf_int',
  @Start    = '10',
  @Stop     = '20',
  @Step     = 10,
  @DataType = 'int',
  @PFRange  = 'LEFT',
	@PSName   = 'ps_int',
  @PSLayout = 'test_2_int_points',
--	@PSAllTo  = 'PRIMARY',
  @PrintOnly= 1
