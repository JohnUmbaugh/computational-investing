import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import eventprofiler as ep

class DiscreteEvent:
	def __init__( self, timestamp_index, symbol, price ):
		self.timestamp_index = timestamp_index
		self.symbol = symbol
		self.price = price

	def to_string( self, ldt_timestamps ):
		return "Event|" + str( self.timestamp_index ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) + "|" + self.symbol + "|" + str(self.price)

class Order:
	def __init__( self, timestamp_index, symbol, share_count, order_type, order_group_id ):
		self.timestamp_index = timestamp_index
		self.symbol = symbol
		self.share_count = share_count
		self.order_type = order_type
		self.order_group_id = order_group_id

	def to_string( self, ldt_timestamps ):
		return "Order|" + str( self.timestamp_index ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) + "|" + self.symbol + "|" + str( self.share_count ) \
			+ "|" + self.order_type + "|" + str( self.order_group_id )

class SimulationEvent:
	def __init__( self, timestamp_index, portfolio_value, cash_on_hand ):
		self.timestamp_index = timestamp_index
		self.portfolio_value = portfolio_value
		self.cash_on_hand = cash_on_hand

	def to_string( self, ldt_timestamps ):
		return "SimEvent|" + str( self.timestamp_index ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) + "|" + str(self.portfolio_value) + "|" + str(self.cash_on_hand)

def find_bollinger_events( ls_symbols, d_data, ldt_timestamps, qualifier ):
	''' Finding the event dataframe '''
	df_close = d_data['close']
	ts_market = df_close['SPY']

	discrete_events = []
	window = 20

	spy_rolling_mean = pd.stats.moments.rolling_mean( ts_market, window)
	spy_rolling_std = pd.stats.moments.rolling_std( ts_market, window)
	spy_bollinger_values = ( ts_market - spy_rolling_mean ) / spy_rolling_std

	print "Finding Events"

	# Creating an empty dataframe
	event_matrix = copy.deepcopy(df_close)
	event_matrix = event_matrix * np.NAN

	for s_sym in ls_symbols:
		rolling_mean = pd.stats.moments.rolling_mean( df_close[s_sym], window)
		rolling_std = pd.stats.moments.rolling_std( df_close[s_sym], window)
		bollinger_values = ( df_close[s_sym] - rolling_mean ) / rolling_std

		value_dict = {
			"bollinger values" : bollinger_values,
			"spy bollinger values" : spy_bollinger_values,
			"rolling std" : rolling_std,
			"close prices" : df_close[s_sym].ix
		}

		for i in range(1, len(ldt_timestamps)):
			# Calculating the returns for this timestamp
			f_symprice_today = df_close[s_sym].ix[ldt_timestamps[i]]
			f_symprice_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]

			if ( qualifier( i, ldt_timestamps, value_dict ) ):
				event_matrix[s_sym].ix[ldt_timestamps[i]] = 1
				discrete_events.append( DiscreteEvent( i, s_sym, f_symprice_today ) )

	sorted_discrete_events = sorted( discrete_events, key = lambda e: ldt_timestamps[ e.timestamp_index ] )

	return event_matrix, sorted_discrete_events

def compute_portfolio( orders, ldt_timestamps, starting_cash ):
	ls_symbols = set()
	for o in orders:
		ls_symbols.add( o.symbol )
	ls_symbols.add( "$SPX" )

	c_dataobj = da.DataAccess("Yahoo")
	ls_keys = [ "close" ]
	ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
	d_data = dict(zip(ls_keys, ldf_data))

	datetime_order_dict = {}

	for i in range( len( orders ) ):
		today_datetime = ldt_timestamps[ orders[ i ].timestamp_index ]
	
		if today_datetime not in datetime_order_dict:
			datetime_order_dict[ today_datetime ] = []

		datetime_order_dict[ today_datetime ].append( orders[ i ] )

	cash = starting_cash
	positions = {}
	for symbol in ls_symbols:
		positions[ symbol ] = 0

	simulation_events = []
	order_group_ids_to_ignore = set()

	for i in range( len( ldt_timestamps ) ):
		today_datetime = ldt_timestamps[ i ]
		today_closings = d_data[ 'close' ].ix[ today_datetime ]

		if today_datetime in datetime_order_dict:
			orders_today = datetime_order_dict[ today_datetime ]
			for order in orders_today:
				if order.order_group_id not in order_group_ids_to_ignore:
					order_closing_price = today_closings[ order.symbol ]
					if order.order_type == "BUY":
						if ( order.share_count * order_closing_price ) > cash:
							# can't buy
							print "too little cash"
							order_group_ids_to_ignore.add( order.order_group_id )
						else:
							positions[ order.symbol ] += order.share_count
							cash -= order.share_count * order_closing_price
					elif order.order_type == "SELL":
						positions[ order.symbol ] -= order.share_count
						cash += order.share_count * order_closing_price				  

		value = cash
		for symbol in ls_symbols:
			if not math.isnan( today_closings[ symbol ] ):
				value += today_closings[ symbol ] * positions[ symbol ]
		simulation_events.append( SimulationEvent( i, value, cash ) )

	return simulation_events

def original_qualifier( i, ldt_timestamps, value_dict ):
	bollinger_values = value_dict[ "bollinger values" ]
	spy_bollinger_values = value_dict[ "spy bollinger values" ]
	if i > 1:
		if bollinger_values[ldt_timestamps[ i - 1 ] ] >= -2.0 \
			and bollinger_values[ldt_timestamps[ i ] ] < -2.0 \
			and spy_bollinger_values[ldt_timestamps[ i ] ] >= 1.1:
				return True
	return False

# few but interesting
def standard_dev_dip_1( i, ldt_timestamps, value_dict ):
	bollinger_values = value_dict[ "bollinger values" ]
	rolling_std = value_dict[ "rolling std" ]
	return bollinger_values[ ldt_timestamps[ i ] ] <= -2.0 \
		and rolling_std[ i ] >= 2.0

def standard_dev_dip( i, ldt_timestamps, value_dict ):
	bollinger_values = value_dict[ "bollinger values" ]
	rolling_std = value_dict[ "rolling std" ]
	return bollinger_values[ ldt_timestamps[ i ] ] <= -1.0

# interesting
def foo( i, ldt_timestamps, value_dict ):
	bollinger_values = value_dict[ "bollinger values" ]
	rolling_std = value_dict[ "rolling std" ]
	closing_prices = value_dict[ "close prices" ]
	if i < 1:
		return false
	return ( closing_prices[ ldt_timestamps[ i ] ] / closing_prices[ ldt_timestamps[ i - 1 ] ] ) < 0.95

# also interesting but rare
def foo2( i, ldt_timestamps, value_dict ):
	bollinger_values = value_dict[ "bollinger values" ]
	rolling_std = value_dict[ "rolling std" ]
	closing_prices = value_dict[ "close prices" ]
	if i < 1:
		return false
	return ( closing_prices[ ldt_timestamps[ i ] ] / closing_prices[ ldt_timestamps[ i - 1 ] ] ) < 0.9

def foo3( i, ldt_timestamps, value_dict ):
	bollinger_values = value_dict[ "bollinger values" ]
	rolling_std = value_dict[ "rolling std" ]
	closing_prices = value_dict[ "close prices" ]
	if i < 1:
		return false
	return ( closing_prices[ ldt_timestamps[ i ] ] / closing_prices[ ldt_timestamps[ i - 1 ] ] ) < 0.98

def convert_events_to_orders( events, ldt_timestamps, trading_days_to_sell_delta = 5, shares_to_transact = 100 ):
	orders = []
	order_group_id = 0
	for e in events:
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )
		orders.append( Order( e.timestamp_index, e.symbol, shares_to_transact, "BUY", order_group_id ) )
		orders.append( Order( sell_timestamp_index, e.symbol, shares_to_transact, "SELL", order_group_id ) )
		order_group_id += 1

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders

if __name__ == '__main__':
	dt_start = dt.datetime(2014, 1, 1)
	dt_end = dt.datetime(2015, 12, 31)
	ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

	dataobj = da.DataAccess('Yahoo', verbose=True, cachestalltime=0)
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

	event_matrix, discrete_events = find_bollinger_events( ls_symbols, d_data, ldt_timestamps, foo )

	for d in discrete_events:
		print d.to_string( ldt_timestamps )

	orders = convert_events_to_orders( discrete_events, ldt_timestamps, 7, 100 )

	for o in orders:
		print o.to_string( ldt_timestamps )

	starting_cash = 100000
	simulation_events = compute_portfolio( orders, ldt_timestamps, starting_cash )

	for e in simulation_events:
		print e.to_string( ldt_timestamps )

	portfolio_value_prior_transaction_cost = simulation_events[ -1 ].portfolio_value
	cost_per_trade = 8.0
	total_trade_cost = cost_per_trade * len( orders )
	final_value = portfolio_value_prior_transaction_cost - total_trade_cost
	profit = final_value - starting_cash
	
	print "portfolio_value_prior_transaction_cost: " + str( portfolio_value_prior_transaction_cost )
	print "total_trade_cost: " + str( total_trade_cost )
	print "final_value: " + str( final_value )
	print "profit: " + str(profit)

	print "Creating Study"
	ep.eventprofiler(event_matrix, d_data, i_lookback=20, i_lookforward=20,
				s_filename='event_study.pdf', b_market_neutral=True, b_errorbars=True,
				s_market_sym='SPY')