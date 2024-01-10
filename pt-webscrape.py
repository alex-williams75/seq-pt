import json
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
import os

url = "https://www.data.qld.gov.au/dataset/translink-origin-destination-trips-2022-onwards"
page = urllib.request.urlopen(url)
html = page.read().decode("utf-8")
soup = BeautifulSoup(html, "html.parser")

df = pd.DataFrame()

print(df)

for script in soup.find_all("script",type="application/ld+json"):
        res_dict = json.loads(script.contents[0])
        for item in res_dict.get('@graph', []):
            url = item.get('schema:url', None)
            if url and url.endswith('.zip'):
                # print(url)
                url_zip = os.path.basename(url)
                zip_csv = url_zip[0:-4]
                print(url)
                print(url_zip)
                print(zip_csv)
                urllib.request.urlretrieve(url, url_zip)
                with zipfile.ZipFile(url_zip, 'r') as ref:
                    ref.extractall()
                csv_ref = [f for f in ref.namelist() if f.endswith('.csv')][0]
                new_data = pd.read_csv(csv_ref)
                df = pd.concat([df, new_data], ignore_index=True)
                os.remove(url_zip)
                os.remove(csv_ref)  
                
df['route'] = df['route'].astype(str)
df['month'] = pd.to_datetime(df['month'], format='%Y-%m')
df['month'] = df['month'].dt.strftime('%Y-%m')
df['origin_stop'] = pd.to_numeric(df['origin_stop'], errors='coerce').astype('Int64')
df['destination_stop'] = pd.to_numeric(df['destination_stop'], errors='coerce').astype('Int64')
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
df = df.sort_values(by = 'month').reset_index(drop=True)

print(df)

df.to_parquet('SEQ_PT_Trips.parquet', index=False)
            

