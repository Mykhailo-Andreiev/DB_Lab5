
-- Task 3: Index creation and testing
CREATE NONCLUSTERED INDEX IX_Allowance_Name ON Allowance(AllowanceName);
SELECT AllowanceName FROM Allowance WHERE AllowanceName = 'Allowance_200';
SELECT AllowanceName FROM Allowance WITH (INDEX(0)) WHERE AllowanceName = 'Allowance_200';

CREATE UNIQUE NONCLUSTERED INDEX IX_Employee_PhoneNumber ON Employee(PhoneNumber);
SELECT * FROM Employee WHERE PhoneNumber = '380000000100';
SELECT * FROM Employee WITH (INDEX(0)) WHERE PhoneNumber = '380000000100';

CREATE NONCLUSTERED INDEX IX_Allowance_Name_Desc ON Allowance(AllowanceName) INCLUDE (Description);
SELECT AllowanceName, Description FROM Allowance WHERE AllowanceName = 'Allowance_100';
SELECT AllowanceName, Description FROM Allowance WITH (INDEX(0)) WHERE AllowanceName = 'Allowance_100';

CREATE NONCLUSTERED INDEX IX_ErrorReport_Emp1_Filtered ON ErrorReport(EmployeeID)
INCLUDE (ErrorType, ErrorDetails) WHERE EmployeeID = 1;
SELECT EmployeeID, ErrorType, ErrorDetails FROM ErrorReport WHERE EmployeeID = 1;
SELECT EmployeeID, ErrorType, ErrorDetails FROM ErrorReport WITH (INDEX(0)) WHERE EmployeeID = 1;

-- Task 5: Index audit query
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    ips.avg_fragmentation_in_percent AS Fragmentation,
    t.name AS TableName,
    ips.page_count AS PageCount
FROM sys.indexes i
JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    ON i.object_id = ips.object_id AND i.index_id = ips.index_id
JOIN sys.tables t
    ON i.object_id = t.object_id
WHERE i.name IS NOT NULL
ORDER BY t.name, i.name;

-- Task 6: System procedures
EXEC sp_helpindex 'Employee';
EXEC sp_spaceused 'Employee';
EXEC sp_columns 'Employee';

-- Task 7: Global temp procedures
CREATE PROCEDURE ##GetEmployeePaymentsByPeriod
    @EmployeeID INT,
    @StartPeriod DATE,
    @EndPeriod DATE
AS
BEGIN
    SELECT * FROM Payment WHERE EmployeeID = @EmployeeID AND Period BETWEEN @StartPeriod AND @EndPeriod;
END;

CREATE PROCEDURE ##GetPositionAllowances
    @PositionID INT
AS
BEGIN
    SELECT A.AllowanceName, A.Description, PA.MaxAllowanceAmount
    FROM Position_Allowance PA
    JOIN Allowance A ON PA.AllowanceID = A.AllowanceID
    WHERE PA.PositionID = @PositionID;
END;

CREATE PROCEDURE ##GetRecentErrors
    @DaysBack INT
AS
BEGIN
    SELECT * FROM ErrorReport WHERE ErrorDate >= DATEADD(DAY, -@DaysBack, GETDATE());
END;

-- Task 8: Local temp procedures
CREATE PROCEDURE #GetAveragePaymentPerEmployee AS
BEGIN
    SELECT E.EmployeeID, E.FirstName, E.LastName,
           ROUND(AVG(P.PaymentAmount), 2) AS AveragePayment
    FROM Employee E
    JOIN Payment P ON E.EmployeeID = P.EmployeeID
    GROUP BY E.EmployeeID, E.FirstName, E.LastName;
END;

CREATE PROCEDURE #GetEmployeePositionAndSalary AS
BEGIN
    SELECT E.EmployeeID, E.FirstName, E.LastName, P.PositionName, P.BaseSalary
    FROM Employee E
    JOIN Position P ON E.PositionID = P.PositionID;
END;

CREATE PROCEDURE #GetAllAllowancesByPosition AS
BEGIN
    SELECT POS.PositionName, A.AllowanceName, PA.MaxAllowanceAmount
    FROM Position_Allowance PA
    JOIN Position POS ON PA.PositionID = POS.PositionID
    JOIN Allowance A ON PA.AllowanceID = A.AllowanceID;
END;

-- Task 9: User procedures with transactions
CREATE PROCEDURE InsertPaymentWithCheck
    @EmployeeID INT,
    @Period DATE,
    @PaymentAmount DECIMAL(10, 2)
AS
BEGIN
    BEGIN TRANSACTION;
    IF EXISTS (SELECT 1 FROM Employee WHERE EmployeeID = @EmployeeID)
    BEGIN
        DECLARE @NextID INT;
        SELECT @NextID = ISNULL(MAX(PaymentID), 0) + 1 FROM Payment;
        INSERT INTO Payment (PaymentID, EmployeeID, Period, PaymentAmount)
        VALUES (@NextID, @EmployeeID, @Period, @PaymentAmount);
        COMMIT TRANSACTION;
    END
    ELSE
    BEGIN
        ROLLBACK TRANSACTION;
        RAISERROR('Працівник не знайдений', 16, 1);
    END
END;

CREATE PROCEDURE UpdateSalaryAndLog
    @PositionID INT,
    @NewSalary DECIMAL(10, 2)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        UPDATE Position SET BaseSalary = @NewSalary WHERE PositionID = @PositionID;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
    END CATCH
END;

CREATE PROCEDURE TransferEmployee
    @EmployeeID INT,
    @NewPositionID INT
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        UPDATE Employee SET PositionID = @NewPositionID WHERE EmployeeID = @EmployeeID;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
    END CATCH
END;

-- Task 10: Insert multiple rows
CREATE PROCEDURE AddEmployeesInBulk
    @RowCount INT
AS
BEGIN
    DECLARE @i INT = 1;
    DECLARE @NextID INT;
    SELECT @NextID = ISNULL(MAX(EmployeeID), 0) + 1 FROM Employee;
    WHILE @i <= @RowCount
    BEGIN
        INSERT INTO Employee (EmployeeID, FirstName, MiddleName, LastName, PhoneNumber, PositionID)
        VALUES (@NextID, CONCAT('Name', @NextID), NULL, CONCAT('Surname', @NextID),
                CONCAT('380000000', FORMAT(@NextID, '00')), 1);
        SET @i = @i + 1;
        SET @NextID = @NextID + 1;
    END
END;

-- Task 11: Insert into Position and return ID
CREATE PROCEDURE InsertPositionAndReturnID
    @PositionName NVARCHAR(100),
    @BaseSalary DECIMAL(10, 2),
    @NewPositionID INT OUTPUT
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        DECLARE @NextID INT;
        SELECT @NextID = ISNULL(MAX(PositionID), 0) + 1 FROM Position;
        INSERT INTO Position (PositionID, PositionName, BaseSalary)
        VALUES (@NextID, @PositionName, @BaseSalary);
        SET @NewPositionID = @NextID;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        SET @NewPositionID = NULL;
    END CATCH
END;
