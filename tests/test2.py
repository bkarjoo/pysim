import pandas as pd
import pickle

# before using this script:
# delete the first rows of the sheet
# the header column is the symbol rows
# fill the symbol row blank column names with the symbol name
# use the vba script in excel for quick fill
# export as csv
df = pd.read_csv('500OHLC.csv')

print df.shape

dates = df.iloc[:, :1]
dates = pd.to_datetime(dates['Dates'])

dfs = {}

i = 1

while i < 2097:
    df2 = df.iloc[:,i:(i+4)]
    df2.index = dates
    symbol = (df2.columns[0]).replace('/','.')
    df2.columns = ['Open', 'High', 'Low', 'Close']
    print df2.head()
    dfs[symbol] = df2
    i += 4

file_handle = open('bb_close.pkl', 'wb')
pickle.dump(dfs, file_handle)
file_handle.close()
