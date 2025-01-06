import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Wczytaj plik CSV
data = pd.read_csv('data_full.csv', sep=',')

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 10)
print(data.head())

data = data.drop(columns=['Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4', 'LAT', 'LON', 'Cross Street', 'Mocodes'])

# Funkcje pomocnicze
# Funkcja do generowania pełnej tabeli wymiaru daty
def generate_full_date_dimension(start_year, end_year):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    all_dates = []

    current_date = start_date
    while current_date <= end_date:
        all_dates.append({
            'Full_Date': current_date,
            'Year': current_date.year,
            'Month': current_date.month,
            'Day': current_date.day,
            'Weekday': current_date.strftime('%A'),
            'Quarter': (current_date.month - 1) // 3 + 1,
        })
        current_date += timedelta(days=1)

    return pd.DataFrame(all_dates)

def extract_date_components(date_str):
    date_obj = datetime.strptime(date_str, '%m/%d/%Y %I:%M:%S %p')
    return {
        'Year': date_obj.year,
        'Month': date_obj.month,
        'Day': date_obj.day,
        'Weekday': date_obj.strftime('%A'),
        'Quarter': (date_obj.month - 1) // 3 + 1,
    }

def extract_time_components(time_int):
    hour = time_int // 100
    minute = time_int % 100
    time_of_day = 'Noc' if hour < 6 else 'Poranek' if hour < 12 else 'Popołudnie'
    return {'Hour': hour, 'Minute': minute, 'Time_Of_Day': time_of_day}

# Wymiary
# Wymiar Data
dim_date = generate_full_date_dimension(2020, 2023)
dim_date['Date_ID'] = range(1, len(dim_date) + 1)

# Tworzenie mapowania dla `Date Rptd`
data['Date Rptd'] = pd.to_datetime(data['Date Rptd'], format='%m/%d/%Y %I:%M:%S %p')
date_rptd_mapping = dim_date[['Date_ID', 'Year', 'Month', 'Day']]
date_rptd_mapping['Full_Date'] = pd.to_datetime(date_rptd_mapping[['Year', 'Month', 'Day']])
date_rptd_dict = dict(zip(date_rptd_mapping['Full_Date'], date_rptd_mapping['Date_ID']))
data['Date_Rptd_ID'] = data['Date Rptd'].map(lambda x: date_rptd_dict.get(x, None))

# Tworzenie mapowania dla `DATE OCC`
data['DATE OCC'] = pd.to_datetime(data['DATE OCC'], format='%m/%d/%Y %I:%M:%S %p')
date_occ_mapping = dim_date[['Date_ID', 'Year', 'Month', 'Day']]
date_occ_mapping['Full_Date'] = pd.to_datetime(date_occ_mapping[['Year', 'Month', 'Day']])
date_occ_dict = dict(zip(date_occ_mapping['Full_Date'], date_occ_mapping['Date_ID']))
data['Date_Occ_ID'] = data['DATE OCC'].map(lambda x: date_occ_dict.get(x, None))

# Wymiar Czas
time_occ = data['TIME OCC'].apply(extract_time_components)
dim_time_occ = pd.DataFrame(time_occ.tolist()).drop_duplicates().reset_index(drop=True)
dim_time_occ['Time_ID'] = range(1, len(dim_time_occ) + 1)

# Dodanie kolumny Full_Time dla mapowania
dim_time_occ['Full_Time'] = dim_time_occ.apply(
    lambda row: f"{int(row['Hour']):02d}:{int(row['Minute']):02d}", axis=1
)

# Tworzenie słownika mapowania Time_ID
time_occ_dict = dict(zip(dim_time_occ['Full_Time'], dim_time_occ['Time_ID']))

# Przypisanie Time_ID do danych
data['Full_Time'] = data['TIME OCC'].astype(str).str.zfill(4).apply(
    lambda x: f"{int(x[:2]):02d}:{int(x[2:]):02d}"  # Konwersja formatu 4-cyfrowego na HH:MM
)
print("Full time:\n"+str(data['Full_Time']))
data['Time_ID'] = data['Full_Time'].map(lambda x: time_occ_dict.get(x, None))

# Usunięcie pomocniczej kolumny Full_Time
data.drop(columns=['Full_Time'], inplace=True)

# Wymiar Obszar
dim_area = data[['AREA', 'AREA NAME', 'Rpt Dist No']].drop_duplicates().reset_index(drop=True)
dim_area.rename(columns={'AREA': 'Area_Code', 'AREA NAME': 'Area_Name', 'Rpt Dist No': 'Rpt_Dist_No'}, inplace=True)
dim_area['Area_ID'] = range(1, len(dim_area) + 1)

# Wymiar Typ przestępstwa
dim_crime = data[['Crm Cd', 'Crm Cd Desc', 'Part 1-2']].drop_duplicates().reset_index(drop=True)
dim_crime.rename(columns={'Crm Cd': 'Crm_Cd', 'Crm Cd Desc': 'Crm_Cd_Desc', 'Part 1-2': 'Part'}, inplace=True)
dim_crime['Crm_Cd_ID'] = range(1, len(dim_crime) + 1)

# Wymiar Ofiary
dim_victim = data[['Vict Age', 'Vict Sex', 'Vict Descent']].drop_duplicates().reset_index(drop=True)
dim_victim.rename(columns={'Vict Age': 'Vict_Age', 'Vict Sex': 'Vict_Sex', 'Vict Descent': 'Vict_Descent'}, inplace=True)
dim_victim['Victim_ID'] = range(1, len(dim_victim) + 1)

# Wymiar Miejsca przestępstw
dim_premis = data[['Premis Cd', 'Premis Desc']].drop_duplicates().reset_index(drop=True)
dim_premis.rename(columns={'Premis Cd': 'Premis_Cd', 'Premis Desc': 'Premis_Desc'}, inplace=True)
dim_premis['Premis_ID'] = range(1, len(dim_premis) + 1)

# Wymiar Broń
dim_weapon = data[['Weapon Used Cd', 'Weapon Desc']].drop_duplicates().reset_index(drop=True)
dim_weapon.rename(columns={'Weapon Used Cd': 'Weapon_Used_Cd', 'Weapon Desc': 'Weapon_Desc'}, inplace=True)
dim_weapon['Weapon_ID'] = range(1, len(dim_weapon) + 1)

# Wymiar Status
dim_status = data[['Status', 'Status Desc']].drop_duplicates().reset_index(drop=True)
dim_status.rename(columns={'Status Desc': 'Status_Desc'}, inplace=True)
dim_status['Status_ID'] = range(1, len(dim_status) + 1)

# Wymiar Lokalizacja
dim_location = data[['LOCATION']].drop_duplicates().reset_index(drop=True)
dim_location['Location_ID'] = range(1, len(dim_location) + 1)

print("Fakty przed laczeniem: \n" + str(data))
print(data.columns)
print(data.dtypes)

# Tabela faktów
fact_table = data.merge(dim_date, left_on='Date_Rptd_ID', right_on='Date_ID', how='left') \
                 .merge(dim_date, left_on='Date_Occ_ID', right_on='Date_ID', how='left') \
                 .merge(dim_time_occ, left_on='Time_ID', right_on='Time_ID', how='left') \
                 .merge(dim_area, left_on=['AREA', 'AREA NAME', 'Rpt Dist No'], right_on=['Area_Code', 'Area_Name', 'Rpt_Dist_No'], how='left') \
                 .merge(dim_victim, left_on=['Vict Age', 'Vict Sex', 'Vict Descent'], right_on=['Vict_Age', 'Vict_Sex', 'Vict_Descent'], how='left') \
                 .merge(dim_premis, left_on='Premis Cd', right_on='Premis_Cd', how='left') \
                 .merge(dim_weapon, left_on='Weapon Used Cd', right_on='Weapon_Used_Cd', how='left') \
                 .merge(dim_status, left_on='Status', right_on='Status', how='left') \
                 .merge(dim_location, left_on='LOCATION', right_on='LOCATION', how='left') \
                 .merge(dim_crime, left_on='Crm Cd', right_on='Crm_Cd', how='left')

print("Dostępne kolumny w fact_table po skonczeniu laczenia:")
print(fact_table.columns)
print(fact_table.dtypes)

fact_table = fact_table[['DR_NO', 'Date_Rptd_ID', 'Date_Occ_ID', 'Time_ID', 'Area_ID', 'Victim_ID',
                         'Premis_ID', 'Weapon_ID', 'Status_ID', 'Location_ID', 'Crm_Cd_ID']]

fact_table.rename(columns={
    'Date_ID_x': 'Date_Rptd_ID',
    'Date_ID_y': 'Date_Occ_ID',
    'Crm_Cd': 'Crm_Cd_ID',
}, inplace=True)
fact_table['Fact_ID'] = range(1, len(fact_table) + 1)

print("Facts: \n" + str(fact_table))
print(fact_table.columns)
#print("DATY: \n" + str(dim_date_rptd))

# Zapis danych do bazy danych
engine = create_engine('mssql+pyodbc://conv:conv@localhost:1433/PHD?driver=ODBC+Driver+17+for+SQL+Server')

fact_table.to_sql('Fact_Incidents', engine, index=False, if_exists='replace')
dim_date.to_sql('Dim_Date', engine, index=False, if_exists='replace')
dim_time_occ.to_sql('Dim_Time', engine, index=False, if_exists='replace')
dim_area.to_sql('Dim_Area', engine, index=False, if_exists='replace')
dim_crime.to_sql('Dim_Crime', engine, index=False, if_exists='replace')
dim_victim.to_sql('Dim_Victim', engine, index=False, if_exists='replace')
dim_premis.to_sql('Dim_Premis', engine, index=False, if_exists='replace')
dim_weapon.to_sql('Dim_Weapon', engine, index=False, if_exists='replace')
dim_status.to_sql('Dim_Status', engine, index=False, if_exists='replace')
dim_location.to_sql('Dim_Location', engine, index=False, if_exists='replace')

print("Dane zostały pomyślnie zapisane w bazie danych!")

# Lista tabel i odpowiadających im kolumn kluczy głównych
tables_and_primary_keys = {
    "Dim_Date": "Date_ID",
    "Dim_Time": "Time_ID",
    "Dim_Area": "Area_ID",
    "Dim_Crime": "Crm_Cd_ID",
    "Dim_Victim": "Victim_ID",
    "Dim_Premis": "Premis_ID",
    "Dim_Weapon": "Weapon_ID",
    "Dim_Status": "Status_ID",
    "Dim_Location": "Location_ID",
    "Fact_Incidents": "Fact_ID",
}

# Mapa kolumn faktów do tabel wymiarów
foreign_keys = {
    'Time_ID': 'Dim_Time',
    'Area_ID': 'Dim_Area',
    'Crm_Cd_ID': 'Dim_Crime',
    'Victim_ID': 'Dim_Victim',
    'Premis_ID': 'Dim_Premis',
    'Weapon_ID': 'Dim_Weapon',
    'Status_ID': 'Dim_Status',
    'Location_ID': 'Dim_Location'
}


# Modyfikacja kolumn na NOT NULL i dodanie kluczy głównych
with engine.connect() as connection:
    for table, primary_key in tables_and_primary_keys.items():
        try:
            # Ustawienie kolumny na NOT NULL
            alter_column_query = text(f"ALTER TABLE {table} ALTER COLUMN {primary_key} BIGINT NOT NULL;")
            connection.execute(alter_column_query)
            print(f"Kolumna {primary_key} w tabeli {table} ustawiona jako NOT NULL.")

            # Dodanie klucza głównego
            alter_key_query = text(f"ALTER TABLE {table} ADD CONSTRAINT PK_{table} PRIMARY KEY ({primary_key});")
            connection.execute(alter_key_query)
            print(f"Klucz główny dodany dla tabeli {table}.")

            connection.commit()
        except Exception as e:
            print(f"Nie udało się zaktualizować tabeli {table}: {e}")

# Tworzenie połączenia z bazą danych
with engine.connect() as connection:
    for fact_column, dimension_table in foreign_keys.items():
        try:
            # Tworzenie zapytania ALTER TABLE dla klucza obcego
            alter_foreign_key_query = text(f"""
                ALTER TABLE Fact_Incidents
                ADD CONSTRAINT FK_Fact_Incidents_{fact_column}
                FOREIGN KEY ({fact_column}) REFERENCES {dimension_table}({fact_column});
            """)

            # Wykonanie zapytania
            connection.execute(alter_foreign_key_query)
            print(f"Klucz obcy dla {fact_column} w tabeli Fact_Incidents został dodany.")
            connection.commit()
        except Exception as e:
            print(f"Nie udało się dodać klucza obcego dla {fact_column}: {e}")

    # Specjalne przypadki
    try:
        # Klucz obcy dla Dim_Occ_Date odnoszący się do Dim_Date
        alter_foreign_key_date_occured_query = text(f"""
                ALTER TABLE Fact_Incidents 
                ADD CONSTRAINT FK_Fact_Incidents_Occured_Date 
                FOREIGN KEY (Date_Occ_ID) 
                REFERENCES Dim_Date (Date_ID);
            """)
        connection.execute(alter_foreign_key_date_occured_query)
        print("Klucz obcy dla Date Occured dodany.")

        # Klucz obcy dla Dim_Rptd_Date odnoszący się do Dim_Date
        alter_foreign_key_date_raported_query = text(f"""
                ALTER TABLE Fact_Incidents 
                ADD CONSTRAINT FK_Fact_Incidents_Raported_Date 
                FOREIGN KEY (Date_Rptd_ID) 
                REFERENCES Dim_Date (Date_ID);
            """)
        connection.execute(alter_foreign_key_date_raported_query)
        print("Klucz obcy dla Date Raported dodany.")
        connection.commit()
    except Exception as e:
        print(f"Nie udało się dodać klucza obcego do tabeli Fact_Incidents: {e}")
