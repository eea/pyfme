DECLARE @SqlStatement NVARCHAR(MAX)

SELECT @SqlStatement =
    COALESCE(@SqlStatement, N'') + N'DROP TABLE [annex_XXIII].' + QUOTENAME(TABLE_NAME) + N';' + CHAR(13)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'annex_XXIII' and TABLE_TYPE = 'BASE TABLE'

PRINT @SqlStatement
