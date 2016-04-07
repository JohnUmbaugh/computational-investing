import numpy as np
import math
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import eventprofiler as ep
import eventfinder
import portfoliosim

class Order:
	def __init__( self, order_id, order_group_id, timestamp_index, symbol, share_count, order_type ):
		self.order_id = order_id
		self.order_group_id = order_group_id
		self.timestamp_index = timestamp_index
		self.symbol = symbol
		self.share_count = share_count
		self.order_type = order_type

	def to_string( self, ldt_timestamps ):
		return "Order|" + str( self.order_id ) + "|" + str( self.order_group_id ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) \
			+ "|" + self.symbol + "|" + str( self.share_count ) + "|" + self.order_type

def convert_events_to_orders_by_share_amount( events, ldt_timestamps, trading_days_to_sell_delta = 5, shares_to_transact = 100 ):
	orders = []
	order_id = 0
	order_group_id = 0
	for e in events:
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )
		orders.append( Order( order_id, order_group_id, e.timestamp_index, e.symbol, shares_to_transact, "BUY" ) )
		order_id += 1
		orders.append( Order( order_id, order_group_id, sell_timestamp_index, e.symbol, shares_to_transact, "SELL" ) )
		order_id += 1
		order_group_id += 1

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders

def convert_events_to_orders( events, ldt_timestamps, trading_days_to_sell_delta = 5, dollar_amount_to_transact = 10000 ):
	orders = []
	order_id = 0
	order_group_id = 0
	for e in events:
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )

		shares_to_transact = math.floor( dollar_amount_to_transact / e.price )

		orders.append( Order( order_id, order_group_id, e.timestamp_index, e.symbol, shares_to_transact, "BUY" ) )
		order_id += 1
		orders.append( Order( order_id, order_group_id, sell_timestamp_index, e.symbol, shares_to_transact, "SELL" ) )
		order_id += 1
		order_group_id += 1

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders

if __name__ == '__main__':
	dt_start = dt.datetime(2014, 1, 1)
	dt_end = dt.datetime(2015, 12, 31)
	ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

	dataobj = da.DataAccess('Yahoo', verbose=True, cachestalltime=10)
	#ls_symbols = dataobj.get_symbols_from_list('symbols')
	ls_symbols = [ 'LUK', 'DIS', 'AMZN', 'KMX', 'MAR', 'CTSH', 'NFLX', 'CSTE', 'ATVI', 'HAS', 'FDX', 'MA', 'OII', 'MKL', 'CNI', 'WDAY', 'DWA', 'WAB', 'AAPL', 'PCLN', 'TRIP', 'AIRM', 'ADBE', 'CLNE', 'GILD', 'EBAY', 'WETF', 'CVS', 'MTH', 'BJRI', 'PII', 'CMI', 'HAIN', 'CGNX', 'SHW', 'BUD', 'BCPC', 'AMG', 'GWR', 'DISCK', 'WWAV', 'NTGR', 'MYL', 'FII', 'F', 'H', 'UHAL', 'XPO', 'PEGA', 'CLB', 'GNRC', 'RPM', 'SWIR', 'GLW' ]
	ls_symbols.append('SPY')

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
	q = eventfinder.ClosingPriceRatioLTThresholdQualifierBuilder( 0.94 ).qualify
	# q = eventfinder.BollingerLTThresholdQualifierBuilder( -3.0 ).qualify
	# q = eventfinder.original_qualifier

	event_matrix, discrete_events = eventfinder.find_events( ls_symbols, d_data, ldt_timestamps, q )

	for d in discrete_events:
		print d.to_string( ldt_timestamps )

	orders = convert_events_to_orders( discrete_events, ldt_timestamps, 7, 16000 )

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
	ep.eventprofiler(event_matrix, d_data, i_lookback=20, i_lookforward=20,
				s_filename='event_study.pdf', b_market_neutral=True, b_errorbars=True,
				s_market_sym='SPY')