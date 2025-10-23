/* 
=====================================================
Stored procedure: Load Bronze Layer (Source -> Bronze)
=====================================================
Script Purpose:
This stored procedure load data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

parameters:
    None.
  This stored procedure dose not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronze;
=====================================================
*/

create OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		PRINT ' =====================================================';
		PRINT 'Loading Bronze Layer';
		PRINT ' =====================================================';
		print'------------------------------------------------------';
		Print 'Loading CRM Tables';
			print'------------------------------------------------------';
			set @batch_start_time = GETDATE();
			set @start_time = GETDATE();
			print ' >> Truncating Table: bronze.crm_cust_info';
			TRUNCATE TABLE bronze.crm_cust_info;

			print '<< Inserting Data Into:bronze.crm_cust_info';
			BULK insert bronze.crm_cust_info
			from 'E:\Data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with(
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
			set @end_time = GETDATE();
			PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' second';
			print'-------------------'
	-------------------------------------------------------------------------------
	set @start_time = GETDATE();
	print ' >> Truncating Table: bronze.crm_product_info';
	TRUNCATE TABLE bronze.crm_product_info;
	print '<< Inserting Data Into:bronze.crm_product_info';
	BULK insert bronze.crm_product_info
	from 'E:\Data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
		set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' second';
	print'-------------------'

	----------------------------------------------------------------------------------------------
	set @start_time = GETDATE();
	print ' >> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	print '<< Inserting Data Into:bronze.crm_sales_details';

	BULK insert bronze.crm_sales_details
	from 'E:\Data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

	set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' second';
	print'-------------------'
	------------------------------------------------------------------
	print'------------------------------------------------------';
	Print 'Loading ERP Tables';
	print'------------------------------------------------------';
	------------------------------------------------------------------
	set @start_time = GETDATE();
	print ' >> Truncating Table: bronze.erp_CUST_AZ12';
	TRUNCATE TABLE bronze.erp_CUST_AZ12;
	print '<< Inserting Data Into:bronze.erp_CUST_AZ12';
	BULK insert bronze.erp_CUST_AZ12
	from 'E:\Data warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with(
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
		set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' second';
	print'-------------------'


	-------------------------------------------------------------
	set @start_time = GETDATE();
	print ' >> Truncating Table: bronze.erp_LOC_A101';
	TRUNCATE TABLE bronze.erp_LOC_A101;
	print '<< Inserting Data Into:bronze.erp_LOC_A101';

	BULK insert bronze.erp_LOC_A101
	from 'E:\Data warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with(
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
		set @end_time = GETDATE();
	PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' second';
	print'-------------------'
	------------------------------------------------------------------------
	set @start_time = GETDATE();
	print ' >> Truncating Table: bronze.erp_PX_CAT_G1V2';
	TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
	print '<< Inserting Data Into:bronze.erp_PX_CAT_G1V2';
	BULK insert bronze.erp_PX_CAT_G1V2
	from 'E:\Data warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
	FIRSTROW=2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
		set @end_time = GETDATE();
			PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time)AS NVARCHAR)+ ' second';
			print'-------------------'
			set @batch_end_time = GETDATE();
			print'========================================='
			print'Loading Bronze Layer is Completed'
			print'-Total Load Duration:'+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) as nvarchar) +'second';

			end try
	   begin catch
		   print'=====================================';
		   print'ERROR OCCURED DURING LOADING BRONZE LAYER';
		   print'ERROR MESSAGE'+ ERROR_MESSAGE();
		   print'ERROR MESSAGE'+CAST(ERROR_NUMBER() AS NVARCHAR);
		   print'ERROR MESSAGE'+CAST(ERROR_STATE() AS NVARCHAR);
		   print'======================================';
				END catch
end
