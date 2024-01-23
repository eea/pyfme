USE [NECPR]
GO
/****** Object:  StoredProcedure [qaqc].[get_completeness_qc_result]    Script Date: 1/22/2024 2:48:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--Parameters: Table_5,Reporting_element,Air quality,Production_or_Use,Use,Estimated_impact_of_biofuels_bioliquids_biomass','Unit','Description_of_methods_to_estimate_the_impact','Start_period','End_period',annexXVI
ALTER PROCEDURE [qaqc].[get_completeness_qc_result]
--Column 1 and 2 are columns with specific values to be filtered. Columns included are the columns actually checked if are null or not
    @ReportNet3HistoricReleaseId INT --= 97
    ,@TableName NVARCHAR(500) --= 'Table_5'
    ,@ColumnName_1 NVARCHAR(500) --= 'Reporting_element'
    ,@ColumnValue_1 NVARCHAR(500) --= 'Air quality'
    ,@ColumnName_2 NVARCHAR(500) --= 'Production_or_Use'
    ,@ColumnValue_2 NVARCHAR(500) --= 'Use'
	,@ColumnName_3 NVARCHAR(500) 
    ,@ColumnValue_3 NVARCHAR(500) 
    ,@ColumnsIncluded NVARCHAR(500) --= '''Estimated_impact_of_biofuels_bioliquids_biomass'',''Unit'',''Description_of_methods_to_estimate_the_impact'',''Start_period'',''End_period'''
    ,@SchemaTable NVARCHAR(100) --= 'annexXVI'
    ,@Result NVARCHAR(20) OUTPUT
	,@sql_statement NVARCHAR(max) OUTPUT
AS
BEGIN
    --PRINT 'Parameters: ' + @TableName + ',' + @ColumnName_1 + ',' + @ColumnValue_1 + ',' + @ColumnName_2 + ',' + @ColumnValue_2 + ',' + @ColumnsIncluded + ',' + @SchemaTable;
    --PRINT '@ColumnsIncluded: ' + @ColumnsIncluded;
    SET @Result  = '-1';
    DECLARE @SQL NVARCHAR(MAX);
	DECLARE @pivoted_table NVARCHAR(50) = 'pivoted_tables';
	DECLARE @Params NVARCHAR(MAX);
	IF (@ColumnName_1 IS NULL AND @ColumnsIncluded is not null)
    BEGIN
        PRINT 'select 0'
        SET @SQL = N'SELECT TOP 1 @ResultOUT = CAST(SUM(c.IsColumnNull) AS NVARCHAR) + ''/'' + CAST(COUNT(1) AS NVARCHAR) 
        FROM [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c
        WHERE c.ColumnName IN (' + @ColumnsIncluded + ')
            AND c.TableName = '''+@TableName+'''
            AND c.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'';
       
        SET @Params = N'@TableName NVARCHAR(100), @ReportNet3HistoricReleaseId INT, @ColumnName_1 NVARCHAR(100),  @ColumnsIncluded NVARCHAR(200), @ResultOUT NVARCHAR(20) OUTPUT, @sql_statement NVARCHAR(max) OUTPUT';
        print @SQL;
        EXEC sp_executesql @SQL, @Params, @TableName, @ReportNet3HistoricReleaseId, @ColumnName_1,  @ColumnsIncluded, @Result OUTPUT, @SQL OUTPUT;
    END;
	else
			begin    

IF (@ColumnName_1 IS NOT NULL AND @ColumnValue_1 IS NOT NULL
        AND (@ColumnName_2 IS  NULL OR @ColumnValue_2 IS NULL))
    BEGIN
        PRINT 'select 1'
        SET @SQL = N'SELECT TOP 1 @ResultOUT = CAST(SUM(c.IsColumnNull) AS NVARCHAR) + ''/'' + CAST(COUNT(1) AS NVARCHAR) 
        FROM [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c
        WHERE c.Table_Id IN (
                SELECT c1.Table_Id
                FROM [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c1
                WHERE c1.TableName = '''+@TableName+'''
                    AND c1.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'
                    AND (c1.ColumnName = '''+@ColumnName_1+''' AND c1.ColumnValue IN ('+@ColumnValue_1+'))
            )
            AND c.ColumnName IN (' + @ColumnsIncluded + ')
            AND c.TableName = '''+@TableName+'''
            AND c.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'';
       
        SET @Params = N'@TableName NVARCHAR(100), @ReportNet3HistoricReleaseId INT, @ColumnName_1 NVARCHAR(100), @ColumnValue_1 NVARCHAR(100), @ColumnsIncluded NVARCHAR(200), @ResultOUT NVARCHAR(20) OUTPUT, @sql_statement NVARCHAR(max) OUTPUT';
        print @SQL;
        EXEC sp_executesql @SQL, @Params, @TableName, @ReportNet3HistoricReleaseId, @ColumnName_1, @ColumnValue_1, @ColumnsIncluded, @Result OUTPUT, @SQL OUTPUT;
    END;
	else
			begin    
    IF (@ColumnName_1 IS NOT NULL AND @ColumnValue_1 IS NOT NULL
        AND (@ColumnName_2 IS NOT NULL OR @ColumnValue_2 IS NOT NULL)
		AND (@ColumnName_3 IS NULL OR @ColumnValue_3 IS NULL))
    BEGIN
        PRINT 'select 2'
        SET @SQL = N'SELECT TOP 1 @ResultOUT = CAST(SUM(c.IsColumnNull) AS NVARCHAR) + ''/'' + CAST(COUNT(1) AS NVARCHAR) 
        FROM [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c
        WHERE c.Table_Id IN (
                SELECT c1.Table_Id
                FROM [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c1
                LEFT JOIN [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c2 ON c1.Table_Id = c2.Table_Id
                WHERE c1.TableName = '''+@TableName+'''
                    AND c2.TableName = '''+@TableName+'''
                    AND c1.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'
                    AND c2.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'
                    AND (c1.ColumnName = '''+@ColumnName_1+''' AND c1.ColumnValue IN ('+@ColumnValue_1+'))
                    AND (c2.ColumnName = '''+@ColumnName_2+''' AND c2.ColumnValue IN ('+@ColumnValue_2+'))
            )
            AND c.ColumnName IN (' + @ColumnsIncluded + ')
            AND c.TableName = '''+@TableName+'''
            AND c.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'';
       
        SET @Params = N'@TableName NVARCHAR(100), @ReportNet3HistoricReleaseId INT, @ColumnName_1 NVARCHAR(100), @ColumnValue_1 NVARCHAR(100), @ColumnName_2 NVARCHAR(100), @ColumnValue_2 NVARCHAR(100), @ColumnsIncluded NVARCHAR(200), @ResultOUT NVARCHAR(20) OUTPUT, @sql_statement NVARCHAR(max) OUTPUT';
        print @SQL;
        EXEC sp_executesql @SQL, @Params, @TableName, @ReportNet3HistoricReleaseId, @ColumnName_1, @ColumnValue_1, @ColumnName_2, @ColumnValue_2, @ColumnsIncluded, @Result OUTPUT, @SQL OUTPUT;
    END;
	else
			begin
				IF (@ColumnName_1 IS NOT NULL AND @ColumnValue_1 IS NOT NULL
				AND (@ColumnName_2 IS NOT NULL OR @ColumnValue_2 IS NOT NULL)
				AND (@ColumnName_3 IS NOT NULL OR @ColumnValue_3 IS NOT NULL))
			BEGIN
				PRINT 'select 3'
				SET @SQL = N'SELECT TOP 1 @ResultOUT = CAST(SUM(c.IsColumnNull) AS NVARCHAR) + ''/'' + CAST(COUNT(1) AS NVARCHAR) 
				FROM [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c
				WHERE c.Table_Id IN (
						SELECT c1.Table_Id
						FROM [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c1
						LEFT JOIN [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c2 ON c1.Table_Id = c2.Table_Id
						LEFT JOIN [NECPR].[' + @SchemaTable + '].['+@pivoted_table+'] c3 ON c1.Table_Id = c3.Table_Id
						WHERE c1.TableName = '''+@TableName+'''
							AND c2.TableName = '''+@TableName+'''
							AND c1.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'
							AND c2.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'
							AND c3.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'
							AND (c1.ColumnName = '''+@ColumnName_1+''' AND c1.ColumnValue IN ('+@ColumnValue_1+'))
							AND (c2.ColumnName = '''+@ColumnName_2+''' AND c2.ColumnValue IN ('+@ColumnValue_2+'))
							AND (c3.ColumnName = '''+@ColumnName_3+''' AND c3.ColumnValue IN ('+@ColumnValue_3+'))
					)
					AND c.ColumnName IN (' + @ColumnsIncluded + ')
					AND c.TableName = '''+@TableName+'''
					AND c.ReportNet3HistoricReleaseId = '+cast(@ReportNet3HistoricReleaseId as nvarchar)+'';

				
				SET @Params = N'@TableName NVARCHAR(100), @ReportNet3HistoricReleaseId INT, @ColumnName_1 NVARCHAR(100), @ColumnValue_1 NVARCHAR(100), @ColumnName_2 NVARCHAR(100), @ColumnValue_2 NVARCHAR(100),@ColumnName_3 NVARCHAR(100), @ColumnValue_3 NVARCHAR(100), @ColumnsIncluded NVARCHAR(200), @ResultOUT NVARCHAR(20) OUTPUT, @sql_statement NVARCHAR(max) OUTPUT';
				print @SQL;
				EXEC sp_executesql @SQL, @Params, @TableName, @ReportNet3HistoricReleaseId, @ColumnName_1, @ColumnValue_1, @ColumnName_2, @ColumnValue_2, @ColumnName_3, @ColumnValue_3, @ColumnsIncluded, @Result OUTPUT, @SQL OUTPUT;
			END;
			end;
	end
end    
    PRINT 'Result: ' + @Result;
END;
