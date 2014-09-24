﻿CREATE FUNCTION [dbo].[MinTimeGuid](
	@DateTime NVARCHAR(30)
)
RETURNS UNIQUEIDENTIFIER
WITH EXECUTE AS CALLER
AS 
EXTERNAL NAME [TimeGuid].[UserDefinedFunctions].[MinTimeGuid];
GO
