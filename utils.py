import pandas as pd
import numpy as np

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
    pd.read_csv('./data/IRLTLT01GBM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','GreatBritain10Y']),
    pd.read_csv('./data/IRLTLT01AUM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Australia10Y']),
    pd.read_csv('./data/IRLTLT01CAM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Canada10Y']),
    pd.read_csv('./data/IRLTLT01CHM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Switzerland10Y']),
    pd.read_csv('./data/IRLTLT01CLM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Chile10Y']),
    pd.read_csv('./data/IRLTLT01DEM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Germany10Y']),
    pd.read_csv('./data/IRLTLT01ESM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Spain10Y']),
    pd.read_csv('./data/IRLTLT01EZM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Eurozone10Y']),
    pd.read_csv('./data/IRLTLT01FRM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','France10Y']),
    pd.read_csv('./data/IRLTLT01ILM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Israel10Y']),
    pd.read_csv('./data/IRLTLT01JPM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Japan10Y']),
    pd.read_csv('./data/IRLTLT01KRM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','SouthKorea10Y']),
    pd.read_csv('./data/IRLTLT01MXM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Mexico10Y']),
    pd.read_csv('./data/IRLTLT01NZM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','NewZealand10Y']),
    pd.read_csv('./data/IRLTLT01SEM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','Sweden10Y']),
    pd.read_csv('./data/IRLTLT01USM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','UnitedStates10Y']),
    pd.read_csv('./data/IRLTLT01ZAM156N.csv', index_col='observation_date', skiprows=[0], names=['observation_date','SouthAfrica10Y']),
], axis=1)
yields = yields.sort_index()
yields = yields.dropna(how='any',axis=0)

### GDP
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
