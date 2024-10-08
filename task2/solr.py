import pandas as pd
import pysolr
from collections import Counter
import requests

csv_file_path = 'Employee_Sample_Data_1.csv'

def checkIfCollection(collectionName):
    solr_base_url = "http://localhost:8983/solr/admin/cores?action=STATUS"
    response = requests.get(solr_base_url)
    if response.status_code == 200:
        collections = response.json()['status']
        # print("Available collections: ", collections.keys())
        if collectionName in collections.keys():
            return True
        else:
            return False
    else:
        print("Failed to check existence")
    
def createCollection(collectionName):
    res = checkIfCollection(collectionName)
    if res:
        print(f'{collectionName} collection already exists!')
        return

    solr_base_url = "http://localhost:8983/solr/admin/cores"

    params = {
        'action': 'CREATE',
        'name': collectionName,
        'instanceDir': collectionName,  
        'configSet': '_default',     
    }
    try:
        response = requests.get(solr_base_url, params=params)
        if response.status_code == 200:
            print(f"Collection '{collectionName}' created successfully.")
        else:
            print(f"Error creating collection '{collectionName}': {response.content}")
    except Exception as e:
        print(f"An error occurred while creating collection '{collectionName}': {e}")

def deleteCollection(collectionName):
    if not checkIfCollection(collectionName):
        print("No such collection exists!")
        return
    solr_base = "http://localhost:8983/solr/admin/cores"
    todelete = f"{solr_base}?action=UNLOAD&core={collectionName}&deleteIndex=true"
    response = requests.get(todelete)
    if response.status_code == 200:
        print(f"Collection {collectionName} deleted successfully!")
    else:
        print(f"Failed to delete collection '{collectionName}'!")

def solrConfig(collectionName):
    global solr
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collectionName}')

def index_data_from_csv(collectionName, col):
    try:
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_file_path, encoding='utf-8', errors='replace') 

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

    if col in df.columns:
        df = df.drop(col, axis=1)

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
    if not department_counts:
        print(f"No records found for the field department in collection {collectionName}!")
    else:
        print(f'Number of employees in each department from {collectionName} collections')
        for i, j in department_counts.items():
            if i == "NaN":
                continue
            print(f"Department: {i} ; Employee Count: {j}")

def searchByColumn(collectionName, p_column_name, p_column_value):
    solrConfig(collectionName)
    query = f"{p_column_name}:{p_column_value}"
    results = list(solr.search(query))
    if not results:
        print("No records found!")
    else:
        print("SEARCH RESULTS")
        ind = 1
        for result in results:
            print(f"\n----Data {ind} ----")
            ind += 1
            for i, j in result.items():
                print(f"{i}:{j}",end=" ")

def getEmpCount(collectionName):
    solrConfig(collectionName)
    results = solr.search('*:*', rows=0)
    print(f"Employee Count in {collectionName} collection: {results.hits}")

def delEmpById(collectionName, p_employee_id):
    solrConfig(collectionName)
    results = solr.search(f"Employee_ID:{p_employee_id}")
    if not results:
        print("No record with such id")
        return
    solr.delete(q=f"Employee_ID:{p_employee_id}")
    print(f"Deleted employee with ID: {p_employee_id} in collection {collectionName}")
    solr.commit()


#function calls

v_nameCollection = "HASH_4892"
v_phoneCollection = "HASH_MUKILSIJU"

deleteCollection(v_nameCollection)
deleteCollection(v_phoneCollection)

createCollection(v_nameCollection)
createCollection(v_phoneCollection)

print("\n1.getEmpCount(v_nameCollection)")
getEmpCount(v_nameCollection)

print("\n2.indexData(v_nameCollection,Department)")
index_data_from_csv(v_nameCollection,'Department')

print("\n3.indexData(v_ phoneCollection, Gender)")
index_data_from_csv(v_phoneCollection,'Gender')

print("\n4.delEmpById (v_ nameCollection ,E02003)")
delEmpById(v_nameCollection,"E02003")

print("\n5.getEmpCount(v_nameCollection)")
getEmpCount(v_nameCollection)

print("\n6.searchByColumn(v_nameCollection,Department,IT)")
searchByColumn(v_nameCollection,'Department','IT')

print("\n7.searchByColumn(v_nameCollection,Gender ,Male)")
searchByColumn(v_nameCollection,'Gender','Male')

print("\n8.searchByColumn(v_ phoneCollection,Department,IT)")
searchByColumn(v_phoneCollection,'Department','IT')

print("\n9.getDepFacet(v_nameCollection)")
get_employee_count_by_department(v_nameCollection)

print("\n10.getDepFacet(v_phoneCollection)")
get_employee_count_by_department(v_phoneCollection)
