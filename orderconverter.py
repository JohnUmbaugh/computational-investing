import math

class OrderGroup:
	next_id = 0

	def __init__( self ):
		self.is_valid = True
		self.id = self.__class__.next_id
		self.__class__.next_id += 1

class Order:
	def __init__( self, order_id, order_group, timestamp_index, symbol, share_count, order_type ):
		self.order_id = order_id
		self.order_group = order_group
		self.timestamp_index = timestamp_index
		self.symbol = symbol
		self.share_count = share_count
		self.order_type = order_type

	def to_string( self, ldt_timestamps ):
		return "Order|" + str( self.order_id ) + "|" + str( self.order_group.id ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) \
			+ "|" + self.symbol + "|" + str( self.share_count ) + "|" + self.order_type

def convert_events_to_orders_by_share_amount( events, ldt_timestamps, trading_days_to_sell_delta = 5, shares_to_transact = 100 ):
	orders = []
	order_id = 0
	order_group_id = 0
	for e in events:
		order_group = OrderGroup()
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )
		orders.append( Order( order_id, order_group, e.timestamp_index, e.symbol, shares_to_transact, "BUY" ) )
		order_id += 1
		orders.append( Order( order_id, order_group, sell_timestamp_index, e.symbol, shares_to_transact, "SELL" ) )
		order_id += 1

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders

def convert_events_to_orders( events, ldt_timestamps, trading_days_to_sell_delta = 5, dollar_amount_to_transact = 10000 ):
	orders = []
	order_id = 0
	order_group_id = 0
	for e in events:
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )

		shares_to_transact = math.floor( dollar_amount_to_transact / e.price )

		order_group = OrderGroup()
		orders.append( Order( order_id, order_group, e.timestamp_index, e.symbol, shares_to_transact, "BUY" ) )
		order_id += 1
		orders.append( Order( order_id, order_group, sell_timestamp_index, e.symbol, shares_to_transact, "SELL" ) )
		order_id += 1

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders