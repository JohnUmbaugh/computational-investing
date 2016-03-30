import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep

def eventprofiler(df_events_arg, d_data, i_lookback=20, i_lookforward=20,
				s_filename='study', b_market_neutral=True, b_errorbars=True,
				s_market_sym='SPY'):
	''' Event Profiler for an event matix'''
	df_close = d_data['close'].copy()
	df_rets = df_close.copy()

	# Do not modify the original event dataframe.
	# df_events = df_events_arg.copy()
	df_events = df_events_arg

	# NOT WORKS
	# if b_market_neutral == True:
	#	 df_rets = df_rets - df_rets[s_market_sym]
	#	 del df_rets[s_market_sym]
	#	 del df_events[s_market_sym]

	#-----THIS BLOCK WAS CHANGED-------
	if b_market_neutral == True:
		df_rets[s_market_sym] = np.NaN
		df_events[s_market_sym] = np.NaN
	
	tsu.returnize0(df_rets.values)

	df_close = df_close.reindex(columns=df_events.columns)

	# Removing the starting and the end events
	df_events.values[0:i_lookback, :] = np.NaN
	df_events.values[-i_lookforward:, :] = np.NaN

	# Number of events
	i_no_events = int(np.logical_not(np.isnan(df_events.values)).sum())
	assert i_no_events > 0, "Zero events in the event matrix"
	na_event_rets = "False"

	# Looking for the events and pushing them to a matrix
	for i, s_sym in enumerate(df_events.columns):
		for j, dt_date in enumerate(df_events.index):
			if df_events[s_sym][dt_date] == 1:
				na_ret = df_rets[s_sym][j - i_lookback:j + 1 + i_lookforward]
				if type(na_event_rets) == type(""):
					na_event_rets = na_ret
				else:
					na_event_rets = np.vstack((na_event_rets, na_ret))

	if len(na_event_rets.shape) == 1:
		na_event_rets = np.expand_dims(na_event_rets, axis=0)

	# Computing daily rets and retuns
	na_event_rets = np.cumprod(na_event_rets + 1, axis=1)
	na_event_rets = (na_event_rets.T / na_event_rets[:, i_lookback]).T

	# Study Params
	na_mean = np.mean(na_event_rets, axis=0)
	na_std = np.std(na_event_rets, axis=0)
	li_time = range(-i_lookback, i_lookforward + 1)

	# Plotting the chart
	plt.clf()
	plt.axhline(y=1.0, xmin=-i_lookback, xmax=i_lookforward, color='k')
	if b_errorbars == True:
		plt.errorbar(li_time[i_lookback:], na_mean[i_lookback:],
					 yerr=na_std[i_lookback:], ecolor='#0000FF',
					 alpha=0.5)
	plt.plot(li_time, na_mean, linewidth=3, label='mean', color='b')
	plt.xlim(-i_lookback - 1, i_lookforward + 1)
	if b_market_neutral == True:
		plt.title('Market Relative mean return of ' + \
				  str(i_no_events) + ' events')
	else:
		plt.title('Mean return of ' + str(i_no_events) + ' events')
	plt.xlabel('Days')
	plt.ylabel('Cumulative Returns')
	plt.savefig(s_filename, format='pdf')


def find_bollinger_events(ls_symbols, d_data):
	''' Finding the event dataframe '''
	df_close = d_data['close']
	ts_market = df_close['SPY']

	tuples = []
	window = 20

	spy_rolling_mean = pd.stats.moments.rolling_mean( ts_market, window)
	spy_rolling_std = pd.stats.moments.rolling_std( ts_market, window)
	spy_bollinger_value = ( ts_market - spy_rolling_mean ) / spy_rolling_std

	print "Finding Events"

	# Creating an empty dataframe
	df_events = copy.deepcopy(df_close)
	df_events = df_events * np.NAN

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

					df_events[s_sym].ix[ldt_timestamps[i]] = 1
#					tuples.append( ( ldt_timestamps[ i ], s_sym, f_symprice_today ) )

#	sorted_tuples = sorted( tuples, key = lambda t: t[ 0 ] )
#	for t in sorted_tuples:
#		print t

	return df_events


if __name__ == '__main__':
#	dt_start = dt.datetime(2008, 1, 1)
	dt_start = dt.datetime(2012, 1, 1)
	dt_end = dt.datetime(2015, 12, 31)
	ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

	dataobj = da.DataAccess('Yahoo')
	ls_symbols = dataobj.get_symbols_from_list('sp5002012')
#	ls_symbols = dataobj.get_symbols_from_list('sp5002008')
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
	df_events = find_bollinger_events(ls_symbols, d_data)

	print "Creating Study"
	#ep.eventprofiler
	eventprofiler(df_events, d_data, i_lookback=20, i_lookforward=20,
				s_filename='bollinger_band_study.pdf', b_market_neutral=True, b_errorbars=True,
				s_market_sym='SPY')