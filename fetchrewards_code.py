import pandas as pd
import sqlalchemy

# Parse Users file
users_container = []
with gzip.GzipFile('users.json.gz', 'r') as pen:
    for i in pen:
    	users_container .append(json.loads(i))

users_frame = pd.DataFrame(users_container )
users_frame.head(10)
users_frame.dtypes
type(users_frame['_id'][0])
# columns _id , createdDate, lastLogin was not parse correctly. Parse out dictionary (Get Values)

users_frame['_id'] = users_frame['_id'].apply(lambda x: x.get(list(x.keys())[0]))
users_frame['createdDate'] = users_frame['createdDate'].apply(lambda x: x.get(list(x.keys())[0]))
#users_frame['lastLogin'] = users_frame['lastLogin'].apply(lambda x: x.get(list(x.keys())[0]))
# Error Parsing lastLogin because of float

error_index = [] 
for num, i in enumerate(users_frame['lastLogin']):
	try:
		users_frame['lastLogin'].iloc[num] = i.get(list(i.keys())[0])
	except AttributeError:
		error_index.append(num)

len(error_index)
users_frame[users_frame.index.isin(error_index)]
# Validated the error problem is NULL values in lastLogin

# Parse Users file
receipts_container = []
with gzip.GzipFile('receipts.json.gz', 'r') as pen:
    for i in pen:
    	receipts_container .append(json.loads(i))
receipts_frame = pd.DataFrame(receipts_container )
receipts_frame.head(10)
receipts_frame[receipts_frame.columns[5:10]]
receipts_frame[receipts_frame.columns[10:15]]
columns_to_clean= ['_id','dateScanned','finishedDate','modifyDate', 'pointsAwardedDate', 'purchaseDate']
receipts_frame.dtypes

# receipts_frame ['finishedDate'] = receipts_frame ['finishedDate'].apply(lambda x: x.get(list(x.keys())[0]))
# # Error Parsing finishedDate because of float
# receipts_frame ['pointsAwardedDate'] = receipts_frame ['pointsAwardedDate'].apply(lambda x: x.get(list(x.keys())[0]))
# # Error Parsing finishedDate because of float
# receipts_frame ['purchaseDate'] = receipts_frame ['purchaseDate'].apply(lambda x: x.get(list(x.keys())[0]))
# Error Parsing finishedDate because of float

error_index = [] 
for column in columns_to_clean:
    for num, i in enumerate(receipts_frame[column]):
        try:
            receipts_frame[column].iloc[num] = i.get(list(i.keys())[0])
        except AttributeError:
            error_index.append(num)

len(error_index)
receipts_frame[receipts_frame.index.isin(error_index)]


brand_container = []
with gzip.GzipFile('brands.json.gz', 'r') as pen:
    for i in pen:
    	brand_container.append(json.loads(i))
brand_frame = pd.DataFrame(brand_container)
brand_frame.head(10)
brand_frame.dtypes
brand_frame[brand_frame.columns[:4]]
brand_frame[brand_frame.columns[4:]]
type(brand_frame.cpg.iloc[0])
columns_to_clean= ['_id','cpg']
# Errors with cpg ( Has two keys, cpg_id and ref_if ) 
# Split columns
brand_frame['cpg_id'] = ''
brand_frame['cpg_ref_id'] = ''


error_index = [] 
for column in columns_to_clean:
    for num, i in enumerate(brand_frame[column]):
        if len(i) == 1:
            try:
                brand_frame[column].iloc[num] = i.get(list(i.keys())[0])
            except AttributeError:
                error_index.append(num)
        elif len(i) == 2:
            try:
                brand_frame['cpg_id'].iloc[num] = i.get(list(i.keys())[0])
                brand_frame['cpg_ref_id'].iloc[num] = i.get(list(i.keys())[1])
            except (ValueError, KeyError, AttributeError):
                error_index.append(num)

len(error_index)
brand_frame[brand_frame.index.isin(error_index)]


# Realize this will be a lot of work with dictionary with many keys within a single column
# Choose question 1 What are the top 5 brands by receipts scanned for most recent month?
# Parse out receipts_frame column rewardsReceiptItemList

receipts_frame.rewardsReceiptItemList.iloc[0]
item_collections = {}

for num in range(len(receipts_frame.rewardsReceiptItemList)):
    try:
        dict_keys = receipts_frame.rewardsReceiptItemList.iloc[num][0]
        item_collections['id'] = num
        for key in dict_keys:
# If key exists in the dictionary then append the value
            if key in item_collections.keys():
                item_collections[key].append(receipts_frame.rewardsReceiptItemList.iloc[num][0].get(key))
            else:
# If key do not exists in the dictionary
# Create key and list as value
# Append the value 
                item_collections[key] = []
                item_collections[key].append(receipts_frame.rewardsReceiptItemList.iloc[num][0].get(key))
    except TypeError: # Type Error Catch Nulls
            item_collections['id'] = num

# Check the top 5 brand using brandCode that was just parsed out from the rewardsReceiptItemList column    
top_5_items = item_collections['brandCode']
top_5_items = pd.DataFrame(top_5_items, columns=['brandCode'])
top_5_items['count_value'] = 1
# Group it to get count 
top_5_items = top_5_items.groupby('brandCode').agg({'count_value':'count'})
top_brands = top_5_items.sort_values(by='count_value',ascending=False).head(6)
# First item is called Brand so I display 6 lines
top_brands = top.merge(brand_frame, left_on='brandCode', right_on='brandCode', how='left')


# Did it all on Python but if I will have do it in SQL it will be
# much more work and time used since I will have to actually parse out
# the receipts_frame.rewardsReceiptItemList column into an items sold list
# This is how I will have done it 

# Set up handle in from Python to import data into sql server
SERVER = "_________________\SQLEXPRESS"
DATABASE = "master"
DRIVER = "SQL Server Native Client 11.0"
USERNAME = "_________________"
PASSWORD = "_________________"
DATABASE_CONNECTION = (
    f"mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}"
)
ssms_conn = sqlalchemy.create_engine(DATABASE_CONNECTION).connect()


users_frame.to_sql(
    target_table_name, ssms_conn, if_exists="replace", index=False
)
receipts_frame.to_sql(
    target_table_name, ssms_conn, if_exists="replace", index=False
)
brand_frame.to_sql(
    target_table_name, ssms_conn, if_exists="replace", index=False
)
ItemList_frame.to_sql(
    target_table_name, ssms_conn, if_exists="replace", index=False
)

SELECT TOP(5) itemList ORDER BY brandCode DESC;
