#!/usr/bin/env python
# coding: utf-8

# ## Silver_Transformation
# 
# null

# # Silver Transformationm

# In[1]:


today_date = '2026-01-05'


# In[2]:


from pyspark.sql.functions import col

Fabric_bronze_path = 'abfss://Dev_Workspace@onelake.dfs.fabric.microsoft.com/Lakehouse_sales.Lakehouse/Tables/dbo/tblsales_bronze'

df = spark.read.format('delta').load(Fabric_bronze_path).filter(col('processing_date')==today_date)

display(df)


# # Data Cleaning
# # handling Duplicates
# 
# 

# In[3]:


print('Before removing Duplicate',df.count())
df_remove_duplicate = df.drop_duplicates()
print('after removing Duplicate',df.count())


# # Handle Missing Values

# In[4]:


df_dropped = df_remove_duplicate.dropna(subset=['Order_ID','Customer_ID'])


# # Business Transformations

# In[5]:


df_days = df_dropped.withColumn('Delivery_days',(col('Ship_Date')-col('Order_Date')).cast('int'))
display(df_days)


# In[6]:


df_profit_margin = df_days.withColumn('Profit_Margin',col('Profit')/col('Sales'))
display(df_profit_margin)


# ## Writing to Silver Layer

# In[7]:


df_profit_margin.createOrReplaceTempView('t_silver_new_data')


# In[8]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# select * from t_silver_new_data


# In[9]:


Fabric_tblsales_silver = 'abfss://Dev_Workspace@onelake.dfs.fabric.microsoft.com/Lakehouse_sales.Lakehouse/Tables/dbo/tblsales_silver'
try:
    spark.read.format('delta').load(Fabric_tblsales_silver).createOrReplaceTempView('t_tblsales_silver')
except:
    v_create_table = f"""CREATE TABLE IF NOT EXISTS tblsales_silver (
            Row_ID  string ,
            Order_ID  string ,
            Order_Date  date ,
            Ship_Date  date ,
            Ship_Mode  string ,
            Customer_ID  string ,
            Customer_Name  string ,
            Segment  string ,
            Postal_Code  string ,
            City  string ,
            State  string ,
            Country  string ,
            Region  string ,
            Market  string ,
            Product_ID  string ,
            Category  string ,
            Sub_Category  string ,
            Product_Name  string ,
            Sales  DOUBLE ,
            Quantity  int ,
            Discount  DOUBLE ,
            Profit  DOUBLE ,
            Shipping_Cost  DOUBLE ,
            Order_Priority  string ,
            Month  string ,
            Year  string ,
            processing_date date,
            Delivery_Days int,
            Profit_Margin DOUBLE

            )"""

    spark.sql(v_create_table)
    spark.read.format('delta').load(Fabric_tblsales_silver).createOrReplaceTempView('t_tblsales_silver')
    
    
    


# In[10]:


sql_statement = f"""MERGE INTO tblsales_silver as target
					USING t_silver_new_data as source
					on target.Order_ID = source.Order_ID and target.Customer_ID = source.Customer_ID

					WHEN MATCHED THEN
						UPDATE SET 
						target.Row_ID = source.Row_ID,
						target.Order_ID  = source.Order_ID,
						target.Order_Date = source.Order_Date,
						target.Ship_Date  = source.Ship_Date,
						target.Ship_Mode  =  source.Ship_Mode,
						target.Customer_ID = source.Customer_ID,
						target.Customer_Name  = source.Customer_Name,
						target.Segment  = source.Segment,
						target.Postal_Code  = source.Postal_Code ,
						target.City  = source.City,
						target.State  = source.State ,
						target.Country  = source.Country ,
						target.Region  = source.Region,
						target.Market  = source.Market ,
						target.Product_ID  = source.Product_ID,
						target.Category  = source.Category,
						target.Sub_Category  = source.Sub_Category ,
						target.Product_Name  = source.Product_Name,
						target.Sales  = source.Sales,
						target.Quantity  = source.Quantity ,
						target.Discount  = source.Discount,
						target.Profit  = source.Profit,
						target.Shipping_Cost  = source.Shipping_Cost,
						target.Order_Priority  = source.Order_Priority,
						target.Month  = source.Month,
						target.Year  = source.Year ,
						target.processing_date = source.processing_date,
                        target.Delivery_Days = source.Delivery_Days,
                        target.Profit_Margin = source.Profit_Margin



					WHEN NOT MATCHED THEN
						INSERT (Row_ID,
								Order_ID,
								Order_Date,
								Ship_Date,
								Ship_Mode,
								Customer_ID,
								Customer_Name,
								Segment,
								Postal_Code,
								City,
								State,
								Country,
								Region,
								Market,
								Product_ID,
								Category,
								Sub_Category,
								Product_Name,
								Sales,
								Quantity,
								Discount,
								Profit,
								Shipping_Cost,
								Order_Priority,
								Month,
								Year,
								processing_date,
                                Delivery_Days,
                                Profit_Margin)
								VALUES
								(
								source.Row_ID,
								source.Order_ID,
								source.Order_Date,
								source.Ship_Date,
								source.Ship_Mode,
								source.Customer_ID,
								source.Customer_Name,
								source.Segment,
								source.Postal_Code,
								source.City,
								source.State,
								source.Country,
								source.Region,
								source.Market,
								source.Product_ID,
								source.Category,
								source.Sub_Category,
								source.Product_Name,
								source.Sales,
								source.Quantity,
								source.Discount,
								source.Profit,
								source.Shipping_Cost,
								source.Order_Priority,
								source.Month,
								source.Year,
								source.processing_date,
                                source.Delivery_Days,
                                source.Profit_Margin
								)"""
spark.sql(sql_statement).show()


# In[11]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# select * from tblsales_silver

