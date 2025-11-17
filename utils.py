import pandas as pd
import numpy as np
import requests
from io import StringIO

### Functions
# Need to convert quarterly notation to day of month in gdp df
def convert_quarterly_to_monthly(quarter : str) -> str:
    qmap = {
        'Q1' : "04-01",
        'Q2' : "07-01",
        'Q3' : "10-01",
        'Q4' : "01-01",
    }
    return quarter[:5] + qmap[quarter[5:]]

def convert_monthly_to_quarterly(monthly : str) -> str:
    mmap = {
        '01-01' : "Q4",
        '04-01' : "Q1",
        '07-01' : "Q2",
        '10-01' : "Q3",
    }
    return monthly[:5] + mmap[monthly[5:]]

### CC mappings
OECD_CODE_MAPPING = {
    "GBR" : "GB",
    "CAN" : "CA",
    "AUS" : "AU",
    "CHE" : "CH",
    "KOR" : "KR",
    "NZL" : "NZ",
    "ZAF" : "ZA",
    "SWE" : "SE",
    "JPN" : "JP",
    "DEU" : "DE",
    "USA" : "US",
    "MEX" : "MX",
    "CHL" : "CL",
    "ESP" : "ES",
    "FRA" : "FR",
    "ISR" : "IL"
}

CC_NAME_MAPPING = {
    "GB" : "Great Britain",
    "AU" : "Australia",
    "CA" : "Canada",
    "CH" : "Switzerland",
    "CL" : "Chile",
    "DE" : "Germany",
    "ES" : "Spain",
    "EZ" : "Eurozone",
    "I9" : "Eurozone",
    "FR" : "France",
    "IL" : "Israel",
    "JP" : "Japan",
    "KR" : "South Korea",
    "MX" : "Mexico",
    "NZ" : "New Zealand",
    "SE" : "Sweden",
    "US" : "United States",
    "ZA" : "South Africa",
}

### SPOT EXCHANGE RATES
# name ERs as domestic currency needed to buy 1 unit of foreign
# data source: https://fred.stlouisfed.org/categories/94
spot_usdeur = pd.read_csv('./data/DEXUSEU.csv', index_col='observation_date')  # Eurozone
spot_yenusd = pd.read_csv('./data/DEXJPUS.csv', index_col='observation_date')  # Japan
spot_chyusd = pd.read_csv('./data/DEXCHUS.csv', index_col='observation_date')  # China
spot_cadusd = pd.read_csv('./data/DEXCAUS.csv', index_col='observation_date')  # Canada
spot_wonusd = pd.read_csv('./data/DEXKOUS.csv', index_col='observation_date')  # South Korea
spot_mxnusd = pd.read_csv('./data/DEXMXUS.csv', index_col='observation_date')  # Mexico
spot_usdukg = pd.read_csv('./data/DEXUSUK.csv', index_col='observation_date')  # United Kingdom
spot_bzrusd = pd.read_csv('./data/DEXBZUS.csv', index_col='observation_date')  # Brazil
spot_dkkusd = pd.read_csv('./data/DEXDNUS.csv', index_col='observation_date')  # Denmark
spot_hkdusd = pd.read_csv('./data/DEXHKUS.csv', index_col='observation_date')  # Hong Kong
spot_inrusd = pd.read_csv('./data/DEXINUS.csv', index_col='observation_date')  # India
spot_myrusd = pd.read_csv('./data/DEXMAUS.csv', index_col='observation_date')  # Malaysia
spot_nokusd = pd.read_csv('./data/DEXNOUS.csv', index_col='observation_date')  # Norway
spot_sekusd = pd.read_csv('./data/DEXSDUS.csv', index_col='observation_date')  # Sweden
spot_chfusd = pd.read_csv('./data/DEXSFUS.csv', index_col='observation_date')  # Switzerland
spot_sgdusd = pd.read_csv('./data/DEXSIUS.csv', index_col='observation_date')  # Singapore
spot_lkrusd = pd.read_csv('./data/DEXSLUS.csv', index_col='observation_date')  # Sri Lanka
spot_zarusd = pd.read_csv('./data/DEXSZUS.csv', index_col='observation_date')  # South Africa
spot_twdusd = pd.read_csv('./data/DEXTAUS.csv', index_col='observation_date')  # Taiwan
spot_thbusd = pd.read_csv('./data/DEXTHUS.csv', index_col='observation_date')  # Thailand
spot_usdaln = pd.read_csv('./data/DEXUSAL.csv', index_col='observation_date')  # Australia
spot_usdnzd = pd.read_csv('./data/DEXUSNZ.csv', index_col='observation_date')  # New Zealand
spot_vesusd = pd.read_csv('./data/DEXVZUS.csv', index_col='observation_date')  # Venezuela

# Convert to USD per foreign currency (inverse of foreign per USD, index_col='observation_date')
spot_eurusd = 1 / spot_usdeur
spot_ukgusd = 1 / spot_usdukg
spot_alnusd = 1 / spot_usdaln
spot_nzdusd = 1 / spot_usdnzd

usd_ind_em  = pd.read_csv('./data/DTWEXEMEGS.csv', index_col='observation_date')
usd_ind_adv = pd.read_csv('./data/DTWEXAFEGS.csv', index_col='observation_date')

allspotsinusd = pd.concat([
    spot_eurusd,
    spot_ukgusd,
    spot_alnusd,
    spot_nzdusd,
    spot_yenusd,
    spot_chyusd,
    spot_cadusd,
    spot_wonusd,
    spot_mxnusd,
    spot_bzrusd,
    spot_dkkusd,
    spot_hkdusd,
    spot_inrusd,
    spot_myrusd,
    spot_nokusd,
    spot_sekusd,
    spot_chfusd,
    spot_sgdusd,
    spot_lkrusd,
    spot_zarusd,
    spot_twdusd,
    spot_thbusd,
],axis=1)
allspotsinusd = allspotsinusd.dropna(axis=0,how='any')
allspotsinusd = allspotsinusd.sort_index()
usd_ind = pd.concat([usd_ind_adv, usd_ind_em],axis=1)
usd_ind = usd_ind.dropna(axis=0,how='any')
usd_ind = usd_ind.sort_index()
usd_ind.columns = ['Advanced','Emerging']

### YIELDS
yields = pd.concat([
    pd.read_csv('./data/IRLTLT01GBM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Great Britain']),
    pd.read_csv('./data/IRLTLT01AUM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Australia']),
    pd.read_csv('./data/IRLTLT01CAM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Canada']),
    pd.read_csv('./data/IRLTLT01CHM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Switzerland']),
    pd.read_csv('./data/IRLTLT01CLM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Chile']),
    pd.read_csv('./data/IRLTLT01DEM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Germany']),
    pd.read_csv('./data/IRLTLT01ESM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Spain']),
    pd.read_csv('./data/IRLTLT01EZM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Eurozone']),
    pd.read_csv('./data/IRLTLT01FRM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','France']),
    pd.read_csv('./data/IRLTLT01ILM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Israel']),
    pd.read_csv('./data/IRLTLT01JPM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Japan']),
    pd.read_csv('./data/IRLTLT01KRM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','South Korea']),
    pd.read_csv('./data/IRLTLT01MXM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Mexico']),
    pd.read_csv('./data/IRLTLT01NZM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','New Zealand']),
    pd.read_csv('./data/IRLTLT01SEM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Sweden']),
    pd.read_csv('./data/IRLTLT01USM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','United States']),
    pd.read_csv('./data/IRLTLT01ZAM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','South Africa']),
], axis=1)
yields = yields.sort_index()
yields = yields.dropna(how='any',axis=0)
# Align format with other OECD data
yields = pd.melt(
    yields.reset_index(),
    id_vars='observation_date'
).set_index('observation_date')

yields.index.name = 'TIME_PERIOD'
yields['REF_AREA'] = yields['variable'].map(
    {v: k for k, v in CC_NAME_MAPPING.items()}
)
yields.columns = ['Country', 'OBS_VALUE_yield', 'REF_AREA']

### YIELD SPREAD WITH US
spread_data = yields.copy()
spread_data['OBS_VALUE_yield'] = pd.to_numeric(spread_data['OBS_VALUE_yield'], errors='coerce')
# Get US yield for each time period
us_yields = spread_data[spread_data['REF_AREA'] == 'US'][['OBS_VALUE_yield']].copy()
us_yields.columns = ['US_yield']
# Merge US yield back to all countries
spread_data = spread_data.merge(us_yields, left_index=True, right_index=True, how='left')
# Calculate yield spread (country yield - US yield)
spread_data['yield_spread'] = spread_data['OBS_VALUE_yield'] - spread_data['US_yield']
# Drop US observations and NaNs
spread_data = spread_data[spread_data['REF_AREA'] != 'US'].dropna()
spread_data = spread_data.sort_index()

### GDP

HOST_URL = "https://sdmx.oecd.org/public/rest/data/"
agency_identifier = "OECD.SDD.NAD"
DSI = "DSD_STES@DF_CLI"
DS_VERSION = "1.1"
start_period = "1990-Q1"

url = HOST_URL + agency_identifier + "," + \
    "DSD_NAMAIN1@DF_QNA_EXPENDITURE_USD" + "," + \
    DS_VERSION + "/Q............?" + \
    f"startPeriod={start_period}&" + \
    "dimensionAtObservation=AllDimensions&" \
    "format=csvfilewithlabels"

resp = requests.get(url=url)
gdp = pd.read_csv(StringIO(resp.text))
print(f"""There are in total
{(gdp['UNIT_MULT'] != 6).sum()} series which do not have
the 6 digit unit multiplier.""")
gdp = gdp.drop(columns=[
    'STRUCTURE','STRUCTURE_ID','STRUCTURE_NAME',
    'ACTION','FREQ','Frequency of observation','ADJUSTMENT','Adjustment',
    'SECTOR','Institutional sector','COUNTERPART_SECTOR','Counterpart institutional sector',
    'TRANSACTION', 'UNIT_MULT', 'Unit multiplier',
    'Transaction', 'INSTR_ASSET',
    'Financial instruments and non-financial assets', 'ACTIVITY',
    'Economic activity', 'EXPENDITURE', 'Expenditure',
    'TABLE_IDENTIFIER', 'Table identifier','Time period',
    'Observation value', 'CONF_STATUS','PRICE_BASE',
    'Confidentiality status', 'DECIMALS', 'Decimals', 'OBS_STATUS',
    'Observation status','Unit multiplier','Price reference year'
])

# Remap REF_AREA to match 2-letter convention + make the time period in monthly format
gdp['REF_AREA'] = gdp['REF_AREA'].map(OECD_CODE_MAPPING)
gdp['Country'] = gdp['REF_AREA'].map(CC_NAME_MAPPING)
gdp = gdp[~(gdp["REF_AREA"]).isna()]
gdp['TIME_PERIOD'] = gdp['TIME_PERIOD'].apply(lambda x : convert_quarterly_to_monthly(x))
gdp = gdp[gdp['Price base'] == 'Current prices']
gdp.rename(columns={'OBS_VALUE': 'OBS_VALUE_gdp'}, inplace=True)

### DERIVE GROWTH RATES
gdp_growth = gdp.pivot_table(
    index='TIME_PERIOD',
    columns='REF_AREA',
    values='OBS_VALUE_gdp'
).dropna(how='any',axis=0).sort_index()
gdp_growth = gdp_growth.pct_change().dropna(how='any',axis=0) * 100

gdp_growth = pd.melt(gdp_growth.reset_index(),id_vars='TIME_PERIOD').set_index('TIME_PERIOD',drop=True)
gdp_growth.columns = ['REF_AREA','OBS_VALUE_growth']

### INFLATION
# Data available monthly for all countries except Australia and New Zealand (quarterly).
agency_identifier = "OECD.SDD.TPS"
url = HOST_URL + agency_identifier + "," + \
    "DSD_PRICES@DF_PRICES_ALL,1.0/.M.N.CPI.._T.N.GY+_Z?" + \
    "format=csvfilewithlabels"

resp = requests.get(url=url)
priceleveldata = pd.read_csv(StringIO(resp.text))

# Remove unnecessary measures
priceleveldata.drop(columns=[
    'STRUCTURE','STRUCTURE_ID','STRUCTURE_NAME',
    'ACTION','FREQ','Frequency of observation','ADJUSTMENT','Adjustment',
    'UNIT_MULT', 'Unit multiplier',
    "DECIMALS", "Decimals",
    "DURABILITY","Durability",
    "BASE_PER", "Base period",
    "METHODOLOGY", "MEASURE", "Measure", "Methodology",
    "Unit of measure",
    "EXPENDITURE", "Expenditure",
    "TRANSFORMATION", "Transformation",
    'Time period', 'Observation value',
    'OBS_STATUS', 'Observation status',
], inplace=True)

# Only keep observations that report the inflation as a net percentage
inflation = priceleveldata[priceleveldata['UNIT_MEASURE']=='PA'].copy()
inflation.drop(columns="UNIT_MEASURE",inplace=True)

inflation['REF_AREA'] = inflation['REF_AREA'].map(OECD_CODE_MAPPING)
inflation = inflation[~(inflation['REF_AREA']).isna()]
inflation = inflation.set_index('TIME_PERIOD',drop=True).sort_index()
inflation.drop(columns='Reference area', inplace=True)
inflation.rename(columns={'OBS_VALUE': 'OBS_VALUE_inflation'}, inplace=True)
inflation.index = inflation.index.astype(str) + '-01'

### PRICE INDICES
# Only keep observations that report the inflation as a net percentage
price_indices = priceleveldata[priceleveldata['UNIT_MEASURE']=='IX'].copy()
price_indices.drop(columns="UNIT_MEASURE",inplace=True)
price_indices['REF_AREA'] = price_indices['REF_AREA'].map(OECD_CODE_MAPPING)
price_indices = price_indices[~(price_indices['REF_AREA']).isna()]
price_indices = price_indices.set_index('TIME_PERIOD',drop=True).sort_index()
price_indices.drop(columns='Reference area', inplace=True)
price_indices.rename(columns={'OBS_VALUE': 'OBS_VALUE_price'}, inplace=True)
price_indices.index = price_indices.index.astype(str) + '-01'

### ECONOMY OPENNESS MEASURES
url = HOST_URL + agency_identifier + "," + \
    "DSD_BOP@DF_TIS,1.0/..S...A.USD_EXC.?" + \
    "format=csvfilewithlabels"
resp = requests.get(url)
trade_services = pd.read_csv(StringIO(resp.text))
goods_services = pd.read_csv('./data/IMF_IMTS.csv')

### Summary on countries analyzed -- given the objective of the exercise
print('Yields are present for the following countries: ')
print(', '.join(list(yields['REF_AREA'].unique())))
print('Only considering these ones in the analysis (data on other countries will be dropped.)')
