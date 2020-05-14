--EXEC sp_changedbowner 'sa'
--EXEC tSQLt.EnableExternalAccess @enable = 0;

EXEC tSQLt.Run 'testSSPM.TestTable1';
EXEC tSQLt.Run 'testSSPM.TestAllTables';

