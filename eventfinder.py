import pandas as pd
import copy
import numpy as np

class DiscreteEvent:
	def __init__( self, timestamp_index, symbol, price ):
		self.timestamp_index = timestamp_index
		self.symbol = symbol
		self.price = price

	def to_string( self, ldt_timestamps ):
		return "Event|" + str( self.timestamp_index ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) + "|" + self.symbol + "|" + str(self.price)

def find_events( ls_symbols, d_data, ldt_timestamps, qualifier ):
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

def original_qualifier( i, ldt_timestamps, value_dict ):
	bollinger_values = value_dict[ "bollinger values" ]
	spy_bollinger_values = value_dict[ "spy bollinger values" ]
	if i > 1:
		if bollinger_values[ldt_timestamps[ i - 1 ] ] >= -2.0 \
			and bollinger_values[ldt_timestamps[ i ] ] < -2.0 \
			and spy_bollinger_values[ldt_timestamps[ i ] ] >= 1.1:
				return True
	return False

class BollingerLTThresholdQualifierBuilder:
	def __init__( self, threshold ):
		self.threshold = threshold
	
	def qualify( self, i, ldt_timestamps, value_dict ):
		bollinger_values = value_dict[ "bollinger values" ]
		return bollinger_values[ ldt_timestamps[ i ] ] <= self.threshold

class ClosingPriceRatioLTThresholdQualifierBuilder:
	def __init__( self, threshold ):
		self.threshold = threshold

	def qualify( self, i, ldt_timestamps, value_dict ):
		closing_prices = value_dict[ "close prices" ]
		if i < 1:
			return false
		return ( closing_prices[ ldt_timestamps[ i ] ] / closing_prices[ ldt_timestamps[ i - 1 ] ] ) < self.threshold