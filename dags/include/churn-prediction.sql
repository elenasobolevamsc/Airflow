DROP TABLE IF EXISTS telecom.tab1;
DROP TABLE IF EXISTS telecom.tab2;
DROP TABLE IF EXISTS telecom.data;

CREATE TABLE telecom.tab1
ENGINE = MergeTree()
ORDER BY customerID
AS
SELECT customerID
	, BeginDate	
	, EndDate
	, Type
	, PaperlessBilling
	, PaymentMethod
	, MonthlyCharges
	, TotalCharges
	, IF(EndDate = 'No', 0, 1) AS Churn
	, IF(EndDate = 'No', '2020-02-01', EndDate) AS EndDate_new
FROM telecom.contract;

ALTER TABLE telecom.tab1 DROP COLUMN EndDate;

ALTER TABLE telecom.tab1 RENAME COLUMN EndDate_new TO EndDate;

ALTER TABLE telecom.tab1
MODIFY COLUMN EndDate DateTime;

CREATE TABLE telecom.tab2
ENGINE = MergeTree()
ORDER BY customerID
AS
SELECT customerID
	, Type
	, PaperlessBilling
	, PaymentMethod
	, MonthlyCharges
    , toFloat64OrZero(TotalCharges) AS TotalCharges
    , dateDiff('day', BeginDate, EndDate) AS Lifetime
    , Churn
FROM telecom.tab1;

DROP TABLE telecom.tab1;

CREATE TABLE telecom.data
ENGINE = MergeTree()
ORDER BY c.customerID
AS 
SELECT
    c.*,
    p.*,
    if(InternetService = '', 'No', InternetService) AS InternetService,
    if(OnlineSecurity = '', 'No', OnlineSecurity) AS OnlineSecurity,
    if(OnlineBackup = '', 'No', OnlineBackup) AS OnlineBackup,
    if(DeviceProtection = '', 'No', DeviceProtection) AS DeviceProtection,
    if(TechSupport = '', 'No', TechSupport) AS TechSupport,
    if(StreamingTV = '', 'No', StreamingTV) AS StreamingTV,
    if(StreamingMovies = '', 'No', StreamingMovies) AS StreamingMovies,
    if(MultipleLines = '', 'No', MultipleLines) AS MultipleLines

FROM telecom.tab2 AS c
LEFT JOIN telecom.personal AS p ON c.customerID = p.customerID
LEFT JOIN telecom.internet AS i ON c.customerID = i.customerID
LEFT JOIN telecom.phone AS ph ON c.customerID = ph.customerID;

ALTER TABLE telecom.data DROP COLUMN p.customerID;

DROP TABLE telecom.tab2;
