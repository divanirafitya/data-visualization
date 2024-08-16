-- dataset
'https://www.kaggle.com/datasets/iamsouravbanerjee/customer-shopping-trends-dataset/data'

-- syntax DDL: table creation
CREATE TABLE table_m3(
	"Customer ID" INT,
	"Age" INT,
	"Gender" VARCHAR(20),
	"Item Purchased" VARCHAR(50),
	"Category" VARCHAR(50),
	"Purchase Amount (USD)" INT,
	"Location" VARCHAR(50),
	"Size" VARCHAR(50),
	"Color" VARCHAR(50),
	"Season" VARCHAR(50),
	"Review Rating" FLOAT,
	"Subscription Status" VARCHAR(50),
	"Shipping Type" VARCHAR(50),
	"Discount Applied" VARCHAR(50),
	"Promo Code Used" VARCHAR(50),
	"Previous Purchases" INT,
	"Payment Method" VARCHAR(50),
	"Frequency of Purchases" VARCHAR(50)
);

-- syntax DDL: fill table values
COPY table_m3(
	"Customer ID",
	"Age",
	"Gender",
	"Item Purchased",
	"Category",
	"Purchase Amount (USD)",
	"Location",
	"Size",
	"Color",
	"Season",
	"Review Rating",
	"Subscription Status",
	"Shipping Type",
	"Discount Applied",
	"Promo Code Used",
	"Previous Purchases",
	"Payment Method",
	"Frequency of Purchases"
)
FROM '/Users/divanirafitya/Desktop/H8/Phase_2/Milestones_3/P2M3_divani_rafitya_data_raw.csv'
DELIMITER ','
CSV HEADER;

-- show table
SELECT * FROM table_m3