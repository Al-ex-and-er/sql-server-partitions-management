--EXEC sp_changedbowner 'sa'
--EXEC tSQLt.EnableExternalAccess @enable = 0;

EXEC tSQLt.Run 'testSSPM.Test_CopyTable_Table1'
EXEC tSQLt.Run 'testSSPM.Test_CopyTable_Heap1'
EXEC tSQLt.Run 'testSSPM.Test_CopyTable_Heap2'
EXEC tSQLt.Run 'testSSPM.Test_CopyTable_Heap3'
EXEC tSQLt.Run 'testSSPM.Test_CopyTable_Heap4'
EXEC tSQLt.Run 'testSSPM.Test_CopyTable_AllTables'

EXEC tSQLt.Run 'testSSPM.Test_Defrag_part_level'
EXEC tSQLt.Run 'testSSPM.Test_Defrag_table_level'

EXEC tSQLt.Run 'testSSPM.Test_F_GetPID'
EXEC tSQLt.Run 'testSSPM.Test_F_GetStartPID'
EXEC tSQLt.Run 'testSSPM.Test_F_GetStopPID'
EXEC tSQLt.Run 'testSSPM.Test_F_GetPIDRange'

EXEC tSQLt.Run 'testSSPM.Test_Create_PF_int'
EXEC tSQLt.Run 'testSSPM.Test_Create_PF_datetime'

EXEC tSQLt.Run 'testSSPM.Test_UpdateStats'
