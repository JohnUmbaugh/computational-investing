import numpy as np
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import eventprofiler as ep
import eventfinder
import portfoliosim
import orderconverter

if __name__ == '__main__':
#	dt_start = dt.datetime(2014, 1, 1)
#	dt_end = dt.datetime(2015, 12, 31)

	dt_start = dt.datetime(2016, 4, 1)
	dt_end = dt.datetime(2016, 4, 22)


	ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

	dataobj = da.DataAccess('Yahoo', verbose=True, cachestalltime=10)
	ls_symbols = dataobj.get_symbols_from_list('symbols')
#	ls_symbols = [ 'LUK', 'DIS', 'AMZN', 'KMX', 'MAR', 'CTSH', 'NFLX', 'CSTE', 'ATVI', 'HAS', 'FDX', 'MA', 'OII', 'MKL', 'CNI', 'WDAY', 'DWA', 'WAB', 'AAPL', 'PCLN', 'TRIP', 'AIRM', 'ADBE', 'CLNE', 'GILD', 'EBAY', 'WETF', 'CVS', 'MTH', 'BJRI', 'PII', 'CMI', 'HAIN', 'CGNX', 'SHW', 'BUD', 'BCPC', 'AMG', 'GWR', 'DISCK', 'WWAV', 'NTGR', 'MYL', 'FII', 'F', 'H', 'UHAL', 'XPO', 'PEGA', 'CLB', 'GNRC', 'RPM', 'SWIR', 'GLW' ]
#	ls_symbols.extend( [ 'HOG', 'DUST', 'TSLA', 'BAC', 'SVC', 'TRTC', 'PGNX' ] )
#	ls_symbols.extend( [ 'SPY' ] )

	ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
	ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys, verbose=True)

	d_data = dict(zip(ls_keys, ldf_data))

	for s_key in ls_keys:
		d_data[s_key] = d_data[s_key].fillna(method='ffill')
		d_data[s_key] = d_data[s_key].fillna(method='bfill')
		d_data[s_key] = d_data[s_key].fillna(1.0)

	# q = eventfinder.ClosingPriceRatioLTThresholdQualifierBuilder( 0.9 ).qualify
	# q = eventfinder.ClosingPriceRatioLTThresholdQualifierBuilder( 0.98 ).qualify
	# q = eventfinder.ClosingPriceRatioLTThresholdQualifierBuilder( 0.95 ).qualify
	q = eventfinder.ClosingPriceRatioLTThresholdQualifierBuilder( 0.95 ).qualify
	# q = eventfinder.BollingerLTThresholdQualifierBuilder( -3.0 ).qualify
	# q = eventfinder.original_qualifier

	event_matrix, discrete_events = eventfinder.find_events( ls_symbols, d_data, ldt_timestamps, q )

	for d in discrete_events:
		print d.to_string( ldt_timestamps )

	orders = orderconverter.convert_events_to_orders_2( discrete_events, ldt_timestamps, 7, 16000 )
#	orders = orderconverter.convert_events_to_orders( discrete_events, ldt_timestamps, 0.025, 1, 10000 )

	for o in orders:
		print o.to_string( ldt_timestamps )

	starting_cash = 100000
	simulation_events, transaction_count = portfoliosim.simulate_portfolio( d_data, orders, ldt_timestamps, starting_cash )

	for e in simulation_events:
		print e.to_string( ldt_timestamps )

	portfolio_value_prior_transaction_cost = simulation_events[ -1 ].portfolio_value
	cost_per_trade = 8.0
	total_trade_cost = cost_per_trade * transaction_count
	final_value = portfolio_value_prior_transaction_cost - total_trade_cost
	profit = final_value - starting_cash
	
	print "portfolio_value_prior_transaction_cost: " + str( portfolio_value_prior_transaction_cost )
	print "transaction_count: " + str( transaction_count )
	print "total_trade_cost: " + str( total_trade_cost )
	print "final_value: " + str( final_value )
	print "profit: " + str(profit)

	print "Creating Study"
#	ep.eventprofiler(event_matrix, d_data, i_lookback=20, i_lookforward=20,
#				s_filename='event_study.pdf', b_market_neutral=True, b_errorbars=True,
#				s_market_sym='SPY')