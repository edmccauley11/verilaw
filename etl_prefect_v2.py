import os
from supabase import create_client, Client
import requests
import ast
import pandas as pd
import re
from prefect import flow, task
from prefect.blocks.system import Secret


secret_block = Secret.load("supabase-url")
url = secret_block.get()


secret_block = Secret.load("supabase-key")
key = secret_block.get()

secret_block = Secret.load("api-key")
API_KEY = secret_block.get()

supabase: Client = create_client(url, key)

# @task
def insert_records(df , table):
    df = df.fillna('')
    data = df.to_dict(orient="records")
    (
        supabase.table(table)
        .insert(data)   # insert multiple rows
        .execute()
    )

# @task
def truncate_table(table):
    (
        supabase.table(table)
        .delete()
        .neq("Id", 0)   # assuming all IDs are >= 1; effectively deletes all
        .execute()
    )

# @task
def fix_chinese_char(x):
    status = bool(re.search(r'[\u4e00-\u9fff]', str(x)))
    if status:
        x = ''
    return x

# @task
def make_name_proper(x):
    if x is None:
        return x
    x = x.split()
    x = " ".join([i.capitalize() for i in x])
    return x

# change table name to Address_Detail
@task
def Company(df):
    final_list = []
    for idx, rows in df.iterrows():
        id = rows["Id"]
        office = rows["Offices"]
        if not isinstance(office, list):
            office = ast.literal_eval(office)
        temp_df = pd.DataFrame(office)
        temp_df["Id"] = id
        final_list.append(temp_df)
    Company = pd.concat(final_list, ignore_index=True)
    Company['Name'] = Company['Name'].apply(make_name_proper)
    Company = Company[['Id', 'OfficeId', 'Name', 'Address1', 'Address2', 'Address3', 'Address4',
        'Postcode', 'Town', 'County', 'Country', 'PhoneNumber', 'Website',
        'Email', 'OfficeType']]
    table = "Address_Detail"
    truncate_table(table)
    print(f'Truncated: {table}')
    insert_records(Company , table)
    print(f'Inserted: {table}')


@task
def WorkArea(df):
    final_list = []
    for idx, rows in df.iterrows():
        id = rows["Id"]
        work = rows["WorkArea"]
        try:
            if not work or pd.isna(work):
                # print('nan')
                continue
        except:
            pass
        if not isinstance(work, list):
            work = ast.literal_eval(work)
        temp_df = pd.DataFrame({"work": work})
        temp_df["Id"] = id
        final_list.append(temp_df)

    final_work_area = pd.concat(final_list, ignore_index=True)
    final_work_area = final_work_area[['Id', 'work']]
    table = "WorkAreas"
    truncate_table(table)
    print(f'Truncated: {table}')
    insert_records(final_work_area , table)
    print(f'Inserted: {table}')


@task
def TradingNames(df):
    final_list = []
    for idx, rows in df.iterrows():
        id = rows["Id"]
        work = rows["TradingNames"]
        try:
            if not work or pd.isna(work):
                # print('nan')
                continue
        except:
            pass
        if not isinstance(work, list):
            work = ast.literal_eval(work)
        # Build a DataFrame for this Id
        temp_df = pd.DataFrame({"TradingNames": work})
        temp_df["Id"] = id
        final_list.append(temp_df)

    # Combine once at the end
    TradingNames = pd.concat(final_list, ignore_index=True)
    TradingNames = TradingNames[['Id', 'TradingNames' ]]
    TradingNames['TradingNames'] = TradingNames['TradingNames'].apply(make_name_proper)
    TradingNames['TradingNames'] = TradingNames['TradingNames'].apply(fix_chinese_char)

    table = "TradingNames"

    truncate_table(table)
    print(f'Truncated: {table}')
    insert_records(TradingNames , table)
    print(f'Inserted: {table}')


@task
def Websites(df):
    final_list = []
    for idx, rows in df.iterrows():
        id = rows["Id"]
        work = rows["Websites"]
        try:
            if not work or pd.isna(work):
                # print('nan')
                continue
        except:
            pass
        if not isinstance(work, list):
            work = ast.literal_eval(work)
        # Build a DataFrame for this Id
        temp_df = pd.DataFrame({"Website": work})
        temp_df["Id"] = id
        final_list.append(temp_df)

    Websites = pd.concat(final_list, ignore_index=True)
    Websites = Websites[['Id','Website']]

    def clean_website(x):
        if pd.isna(x):  # handle NaN
            return x
        x = x.strip()  # remove spaces
        x = x.replace("https://", "").replace("http://", "")
        if not x.startswith("www."):
            x = "www." + x
        return x

    Websites['Website'] = Websites['Website'].apply(clean_website)
    table = "Websites"
    truncate_table(table)
    print(f'Truncated: {table}')
    insert_records(Websites , table)
    print(f'Inserted: {table}')



@task
def PreviousNames(df):
    final_list = []
    for idx, rows in df.iterrows():
        id = rows["Id"]
        work = rows["PreviousNames"]
        try:
            if not work or pd.isna(work):
                # print('nan')
                continue
        except:
            pass
        if not isinstance(work, list):
            work = ast.literal_eval(work)

        temp_df = pd.DataFrame({"PreviousName": work})
        temp_df["Id"] = id
        final_list.append(temp_df)

    # Combine once at the end
    PreviousNames = pd.concat(final_list, ignore_index=True)
    PreviousNames = PreviousNames[['Id', 'PreviousName']]
    PreviousNames['PreviousName'] = PreviousNames['PreviousName'].apply(make_name_proper)
    
    table = "PreviousNames"
    truncate_table(table)
    print(f'Truncated: {table}')
    insert_records(PreviousNames , table)
    print(f'Inserted: {table}')



@task
def Firm(df):
    cols = ['Id', 'SraNumber', 'PracticeName', 'AuthorisationType',
        'AuthorisationStatus', 'OrganisationType', 'AuthorisationDate',
        'AuthorisationStatusDate', 'FreelanceBasis', 'Regulator',
        'ReservedActivites', 'CompanyRegNo', 'Constitution', 'NoOfOffices',
        'Type']    
    Firm = df[cols]
    Firm['PracticeName'] = Firm['PracticeName'].apply(make_name_proper)
    table = "Firm"
    truncate_table(table)
    print(f'Truncated: {table}')
    insert_records(Firm , table)
    print(f'Inserted: {table}')


@task
def get_data_from_api(api_key):
    print('Getting data from the api')
    url = f"https://sra-prod-apim.azure-api.net/datashare/api/V1/organisation/GetAll?subscription-key={api_key}"
    response = requests.get(url)
    my_dict = response.json()["Organisations"]
    df = pd.DataFrame(my_dict)
    df = df[df['AuthorisationStatus'] == 'YES']
    return df


@flow
def etl_run():
    api_key = API_KEY
    df = get_data_from_api(api_key)
    print("Processing tables")
    Firm(df)
    #renmae the company table to Address_Detail
    Company(df)
    WorkArea(df)
    TradingNames(df)
    Websites(df)
    PreviousNames(df)


if __name__ == "__main__":
    etl_run()

    

