import pandas as pd
import pysolr
from collections import Counter


v_nameCollection = "HASH_4892"
v_phoneCollection = "HASH_MUKILSIJU"
csv_file_path = 'Employee_Sample_Data_1.csv'

def solrConfig(collection_name):
    global solr
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}', always_commit=True, timeout=10)


def index_data_from_csv(collectionName, col):
    try:
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_file_path, encoding='utf-8', errors='replace')

    # print("Dataframe: \n", df)

    # Clean the DataFrame
    df['Employee ID'] = df['Employee ID'].str.strip()
    df['Full Name'] = df['Full Name'].str.strip()
    df['Job Title'] = df['Job Title'].str.strip()
    df['Department'] = df['Department'].str.strip()
    df['Business Unit'] = df['Business Unit'].str.strip()
    df['Gender'] = df['Gender'].str.strip()
    df['Ethnicity'] = df['Ethnicity'].str.strip()
    df['Annual Salary'] = df['Annual Salary'].str.replace(r'[\$,]', '', regex=True).astype(float)
    df['Bonus %'] = df['Bonus %'].str.replace('%', '').astype(float)
    df['Exit Date'] = df['Exit Date'].fillna('')

    df = df.drop(col, axis=1)

    # print("\nAfter deletion:")
    # print(df)

    docs = df.to_dict(orient='records')
    solrConfig(collectionName)
    solr.add(docs)
    print(f"Indexed {len(docs)} documents successfully in the collection {collectionName}.")

def get_employee_count_by_department(collectionName):
    solrConfig(collectionName)
    results = solr.search('*:*', fl='Department', rows=1262)  
    departments = []
    for doc in results:
        if 'Department'in doc:
            departments.extend(doc['Department'])
    department_counts = Counter(departments)

    return department_counts

def searchByColumn(collectionName, p_column_name, p_column_value):
    solrConfig(collectionName)
    query = f"{p_column_name}:{p_column_value}"
    results = solr.search(query)
    return list(results)

def getEmpCount(p_collection_name):
    solrConfig(p_collection_name)
    results = solr.search('*:*', rows=0)
    return results.hits

def delEmpById(p_collection_name, p_employee_id):
    solrConfig(p_collection_name)
    solr.delete(id=p_employee_id)
    print(f"Deleted employee with ID: {p_employee_id} in collection {p_collection_name}")



#function calls

print("\n1.getEmpCount(v_nameCollection)")
emp_count = getEmpCount(v_nameCollection)
print(f"\nEmployee Count in {v_nameCollection} collection: {emp_count}")

# print("\n2.indexData(v_nameCollection,Department)")
# index_data_from_csv(v_nameCollection,'Department')

print("\n3.indexData(v_ phoneCollection, Gender)")
index_data_from_csv(v_phoneCollection,'Gender')

print("\n4.delEmpById (v_ nameCollection ,E02003)")
delEmpById(v_nameCollection,"E02003")

print("\n5.getEmpCount(v_nameCollection)")
emp_count = getEmpCount(v_nameCollection)
print(f"\nEmployee Count in {v_nameCollection} collection: {emp_count}")

print("\n6.searchByColumn(v_nameCollection,Department,IT)")
results = searchByColumn(v_nameCollection,'Department','IT')
if not results:
    print("No records found!")
else:
    print("SEARCH RESULTS")
    # print(len(results))
    for result in results:
        print(result)

print("\n7.searchByColumn(v_nameCollection,Gender ,Male)")
results = searchByColumn(v_nameCollection,'Gender','Male')
# print("type: ",type(results))
if not results:
    print("No records found!")
else:
    print("SEARCH RESULTS")
    # print(len(results))
    for result in results:
        print(result)

print("\n8.searchByColumn(v_ phoneCollection,Department,IT)")
results = searchByColumn(v_phoneCollection,'Department','IT')
if not results:
    print("No records found!")
else:
    print("SEARCH RESULTS")
    for result in results:
        print(result)

# 9.getDepFacet(v_ nameCollection)
print("\n9.getDepFacet(v_ nameCollection)")
results = get_employee_count_by_department(v_nameCollection)
if not results:
    print("No records found!")
else:
    print(f'Number of employees in each department from {v_nameCollection} collections')
    for i, j in results.items():
        print("Department:",i,"=",j)

print("\n10.getDepFacet(v_ phoneCollection)")
results = get_employee_count_by_department(v_phoneCollection)
if not results:
    print("Not found")
else:
    print(f'Number of employees in each department from {v_phoneCollection} collections')
    for i, j in results.items():
        print(i,"=",j)  
