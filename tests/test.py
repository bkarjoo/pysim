import pickle

file_handle = open('bb_close.pkl', 'rb')
ddic = pickle.load(file_handle)
file_handle.close()

df = (ddic['GE'])

print df
print df['2017-07-12':'2017-07-12']['Low'][0]