import QSTK.qstkutil.DataAccess as da
import math

class SimulationEvent:
	def __init__( self, timestamp_index, portfolio_value, cash_on_hand ):
		self.timestamp_index = timestamp_index
		self.portfolio_value = portfolio_value
		self.cash_on_hand = cash_on_hand

	def to_string( self, ldt_timestamps ):
		return "SimEvent|" + str( self.timestamp_index ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) + "|" + str(self.portfolio_value) + "|" + str(self.cash_on_hand)

def simulate_portfolio( d_data, orders, ldt_timestamps, starting_cash ):
	transaction_count = 0
	ls_symbols = set()
	for o in orders:
		ls_symbols.add( o.symbol )
	ls_symbols.add( "SPY" )

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
	persistant_orders = set()

	for i in range( len( ldt_timestamps ) ):
		today_datetime = ldt_timestamps[ i ]
		today_closings = d_data[ 'close' ].ix[ today_datetime ]

		if today_datetime in datetime_order_dict:
			orders_today = datetime_order_dict[ today_datetime ]
			for order in orders_today:
				if order.order_group.is_valid:
					order_closing_price = today_closings[ order.symbol ]
					if order.order_action == "BUY":
						if ( order.share_count * order_closing_price ) > cash:
							# can't buy
							print "too little cash"
							order.order_group.is_valid = False
						else:
							positions[ order.symbol ] += order.share_count
							cash -= order.share_count * order_closing_price
							transaction_count += 1
					elif order.order_action == "SELL":
						if order.order_type == "MinimumPriceTargetOrder":
							persistant_orders.add( order )
						elif order.order_type == "Order":
							positions[ order.symbol ] -= order.share_count
							cash += order.share_count * order_closing_price
							transaction_count += 1		  

		orders_to_remove = set()
		for order in persistant_orders:
			order_closing_price = today_closings[ order.symbol ]			
			if order.order_type == "MinimumPriceTargetOrder":
				positions[ order.symbol ] -= order.share_count
				cash += order.share_count * order_closing_price
				orders_to_remove.add( order )
				transaction_count += 1

		for order in orders_to_remove:
			persistant_orders.remove( order )

		value = cash
		for symbol in ls_symbols:
			if not math.isnan( today_closings[ symbol ] ):
				value += today_closings[ symbol ] * positions[ symbol ]
		simulation_events.append( SimulationEvent( i, value, cash ) )

	return simulation_events, transaction_count