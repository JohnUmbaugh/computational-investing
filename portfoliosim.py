import QSTK.qstkutil.DataAccess as da
import math

class SimulationEvent:
	def __init__( self, timestamp_index, portfolio_value, cash_on_hand ):
		self.timestamp_index = timestamp_index
		self.portfolio_value = portfolio_value
		self.cash_on_hand = cash_on_hand

	def to_string( self, ldt_timestamps ):
		return "SimEvent|" + str( self.timestamp_index ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) + "|" + str(self.portfolio_value) + "|" + str(self.cash_on_hand)

def simulate_portfolio( orders, ldt_timestamps, starting_cash ):
	transaction_count = 0
	ls_symbols = set()
	for o in orders:
		ls_symbols.add( o.symbol )
	ls_symbols.add( "SPY" )

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
							transaction_count += 1
					elif order.order_type == "SELL":
						positions[ order.symbol ] -= order.share_count
						cash += order.share_count * order_closing_price
						transaction_count += 1		  

		value = cash
		for symbol in ls_symbols:
			if not math.isnan( today_closings[ symbol ] ):
				value += today_closings[ symbol ] * positions[ symbol ]
		simulation_events.append( SimulationEvent( i, value, cash ) )

	return simulation_events, transaction_count