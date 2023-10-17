-- T-SQL for "Transact-SQL", Microsoft's extension of SQL



use AdventureWorksDW2019
Go                           -- Go is a batch terminator in T-SQL; it signifies the end of a batch of statements.
 CREATE TRIGGER dbo.updateDate
ON [dbo].[customers]
AFTER  UPDATE 
AS UPDATE [dbo].[customers] SET Modified_at = CURRENT_TIMESTAMP
      FROM [dbo].[customers] t
	    INNER JOIN inserted i     -- The inserted table is a special table in SQL Server that holds the new version of the rows that were just updated.
		  ON t.customerId = i.customerId
GO

-- By joining the customers table with the inserted table on the customerId, the trigger ensures that it only updates the rows that were just modified by the last UPDATE statement.