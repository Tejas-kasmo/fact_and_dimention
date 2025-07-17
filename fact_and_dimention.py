import json
from io import BytesIO

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import configparser
import pandas as pd

from sqlalchemy import create_engine
import urllib.parse
from sqlalchemy import text  


config = configparser.ConfigParser()
config.read(r'C:\Users\mysur\OneDrive\Desktop\python_tutorial\venv1\config.config')


site_url = config['SharePoint']['url']
username = config['SharePoint']['username']
password = config['SharePoint']['password']


sql_username = config['ssms']['UID']
sql_password = config['ssms']['PWD']
sql_server = config['ssms']['SERVER']
sql_database = config['ssms']['DATABASE']


encoded_username = urllib.parse.quote_plus(sql_username)
encoded_password = urllib.parse.quote_plus(sql_password)


connection_string = (
    f"mssql+pyodbc://{encoded_username}:{encoded_password}@{sql_server}/{sql_database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)


engine = create_engine(connection_string)


ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))

folder_url = "/sites/kasmo-training/Shared Documents/DATASET/JSON_FILES"
folder = ctx.web.get_folder_by_server_relative_url(folder_url)

files = folder.files
ctx.load(files)
ctx.execute_query()

print(f"Number of files found: {len(files)}")

for file in files:
    file_name = file.properties['Name']
    file_url = file.properties['ServerRelativeUrl']

    file_obj = ctx.web.get_file_by_server_relative_url(file_url)

    download_buffer = BytesIO()
    file_obj.download(download_buffer).execute_query()

    content = download_buffer.getvalue()
    json_str = content.decode('utf-8')
    json_data = json.loads(json_str)

    print(f"\nProcessing file: {file_name}")
    df = pd.DataFrame(json_data)

    table_name = file_name.replace('.json', '').replace(' ', '_')

    if table_name == 'sales_dimensions':

        df_supplier = df[['supplier_id', 'supplier_name', 'contact_email', 'supplier_country', 'reliability_score', 'region_id', 'promotion_id']].copy()
        df_region = df[['region_id', 'region_name', 'region_country', 'regional_manager']].copy()
        df_promotion = df[['promotion_id', 'promotion_name', 'discount_percentage', 'start_date', 'end_date']].copy()
 
        ids = []
        for i in range(1,251):
            ids.append(i)

        ids = pd.DataFrame({'unique_id':ids})

        df_region = pd.concat([ids,
                               df_region],axis=1)
        
        df_promotion = pd.concat([ids,
                                  df_promotion],
                                  axis = 1)
        
        df_supplier.to_sql('supplier', con=engine,if_exists='replace',index=False)
        df_region.to_sql('region', con=engine,if_exists='replace',index=False)
        df_promotion.to_sql('promotion', con=engine,if_exists='replace',index=False)

        print('sub dimentional table uploaded')

        continue

    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f" Uploaded to table: {table_name}")

with engine.connect() as conn:
    
    conn.execute(text("ALTER TABLE store_dimension ALTER COLUMN store_id BIGINT NOT NULL;"))
    conn.execute(text("ALTER TABLE product_dimension ALTER COLUMN product_id BIGINT NOT NULL;"))
    conn.execute(text("ALTER TABLE time_dimension ALTER COLUMN date_id BIGINT NOT NULL;"))
    conn.execute(text("ALTER TABLE supplier ALTER COLUMN supplier_id BIGINT NOT NULL;"))
    conn.execute(text("ALTER TABLE region ALTER COLUMN unique_id BIGINT NOT NULL;"))
    conn.execute(text("ALTER TABLE promotion ALTER COLUMN unique_id BIGINT NOT NULL;"))


    conn.execute(text("ALTER TABLE store_dimension ADD CONSTRAINT PK_store_dimension PRIMARY KEY (store_id);"))
    conn.execute(text("ALTER TABLE product_dimension ADD CONSTRAINT PK_product_dimension PRIMARY KEY (product_id);"))
    conn.execute(text("ALTER TABLE time_dimension ADD CONSTRAINT PK_time_dimension PRIMARY KEY (date_id);"))
    conn.execute(text("ALTER TABLE supplier ADD CONSTRAINT PK_supplier PRIMARY KEY (supplier_id);"))
    conn.execute(text("ALTER TABLE region ADD CONSTRAINT PK_region PRIMARY KEY (unique_id);"))
    conn.execute(text("ALTER TABLE promotion ADD CONSTRAINT PK_promotion PRIMARY KEY (unique_id);"))


    conn.execute(text("""
        ALTER TABLE sales_fact
        ADD CONSTRAINT FK_salesfact_store
        FOREIGN KEY (store_id) REFERENCES store_dimension(store_id);
    """))
    conn.execute(text("""
        ALTER TABLE sales_fact
        ADD CONSTRAINT FK_salesfact_product
        FOREIGN KEY (product_id) REFERENCES product_dimension(product_id);
    """))
    conn.execute(text("""
        ALTER TABLE sales_fact
        ADD CONSTRAINT FK_salesfact_time
        FOREIGN KEY (date_id) REFERENCES time_dimension(date_id);
    """))

    conn.execute(text("""
        ALTER TABLE product_dimension
        ADD CONSTRAINT FK_product_supplier
        FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id);
    """))

    conn.execute(text("""
        ALTER TABLE supplier
        ADD CONSTRAINT FK_supplier_region
        FOREIGN KEY (region_id) REFERENCES region(region_id);
    """))

    conn.execute(text("""
        ALTER TABLE supplier
        ADD CONSTRAINT FK_supplier_promotion
        FOREIGN KEY (promotion_id) REFERENCES promotion(promotion_id);
    """))

    conn.commit()

print(" All PKs and FKs added successfully.")
