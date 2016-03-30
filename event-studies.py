import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import eventprofiler as ep

class DiscreteEvent:
	def __init__( self, timestamp, symbol, price ):
		self.timestamp = timestamp
		self.symbol = symbol
		self.price = price

def find_bollinger_events( ls_symbols, d_data ):
	''' Finding the event dataframe '''
	df_close = d_data['close']
	ts_market = df_close['SPY']

	discrete_events = []
	window = 20

	spy_rolling_mean = pd.stats.moments.rolling_mean( ts_market, window)
	spy_rolling_std = pd.stats.moments.rolling_std( ts_market, window)
	spy_bollinger_value = ( ts_market - spy_rolling_mean ) / spy_rolling_std

	print "Finding Events"

	# Creating an empty dataframe
	event_matrix = copy.deepcopy(df_close)
	event_matrix = event_matrix * np.NAN

	# Time stamps for the event range
	ldt_timestamps = df_close.index

	for s_sym in ls_symbols:
		rolling_mean = pd.stats.moments.rolling_mean( df_close[s_sym], window)
		rolling_std = pd.stats.moments.rolling_std( df_close[s_sym], window)
		bollinger_value = ( df_close[s_sym] - rolling_mean ) / rolling_std

		for i in range(1, len(ldt_timestamps)):
			# Calculating the returns for this timestamp
			f_symprice_today = df_close[s_sym].ix[ldt_timestamps[i]]
			f_symprice_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]

			if i > 1:
				if bollinger_value[ldt_timestamps[ i - 1 ] ] >= -2.0 \
					and bollinger_value[ldt_timestamps[ i ] ] < -2.0 \
					and spy_bollinger_value[ldt_timestamps[ i ] ] >= 1.1:

					event_matrix[s_sym].ix[ldt_timestamps[i]] = 1
					discrete_events.append( DiscreteEvent( ldt_timestamps[ i ], s_sym, f_symprice_today ) )

	sorted_discrete_events = sorted( discrete_events, key = lambda e: e.timestamp )

	return event_matrix, sorted_discrete_events


if __name__ == '__main__':
	dt_start = dt.datetime(2012, 6, 1)
	dt_end = dt.datetime(2015, 12, 31)
	ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

	dataobj = da.DataAccess('Yahoo')
	ls_symbols = dataobj.get_symbols_from_list('sp5002012')
	ls_symbols.append('SPY')

	ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
	ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
	d_data = dict(zip(ls_keys, ldf_data))

	for s_key in ls_keys:
		d_data[s_key] = d_data[s_key].fillna(method='ffill')
		d_data[s_key] = d_data[s_key].fillna(method='bfill')
		d_data[s_key] = d_data[s_key].fillna(1.0)

	#df_events = find_events(ls_symbols, d_data)
	#df_events = find_hw2_events(ls_symbols, d_data)
	#df_events = find_my_events(ls_symbols, d_data)
	event_matrix, discrete_events = find_bollinger_events(ls_symbols, d_data)

	for d in discrete_events:
		print str( d.timestamp ) + " - " + str( d.symbol ) + " - " + str( d.price )

	print "Creating Study"
	ep.eventprofiler(event_matrix, d_data, i_lookback=20, i_lookforward=20,
				s_filename='event_study.pdf', b_market_neutral=True, b_errorbars=True,
				s_market_sym='SPY')