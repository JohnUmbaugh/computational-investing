import math

class OrderGroup:
	next_id = 0

	def __init__( self ):
		self.is_valid = True
		self.id = self.__class__.next_id
		self.__class__.next_id += 1

class Order:
	next_id = 0

	def __init__( self, order_group, timestamp_index, symbol, share_count, order_action ):
		self.id = self.__class__.next_id
		self.__class__.next_id += 1

		self.order_type = "Order"

		self.order_group = order_group
		self.timestamp_index = timestamp_index
		self.symbol = symbol
		self.share_count = share_count
		self.order_action = order_action

	def to_string( self, ldt_timestamps ):
		return "Order|" + str( self.id ) + "|" + str( self.order_group.id ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) \
			+ "|" + self.symbol + "|" + str( self.share_count ) + "|" + self.order_action


class MinimumPriceTargetOrder:
	next_id = 0

	def __init__( self, order_group, timestamp_index, symbol, share_count, order_action, price_target ):
		self.id = self.__class__.next_id
		self.__class__.next_id += 1

		self.order_type = "MinimumPriceTargetOrder"

		self.order_group = order_group
		self.timestamp_index = timestamp_index
		self.symbol = symbol
		self.share_count = share_count
		self.order_action = order_action

		self.price_target = price_target

	def to_string( self, ldt_timestamps ):
		return "MinimumPriceTargetOrder|" + str( self.id ) + "|" + str( self.order_group.id ) + "|" + str( ldt_timestamps[ self.timestamp_index ] ) \
			+ "|" + self.symbol + "|" + str( self.share_count ) + "|" + self.order_action

def convert_events_to_orders_by_share_amount( events, ldt_timestamps, trading_days_to_sell_delta = 5, shares_to_transact = 100 ):
	orders = []
	for e in events:
		order_group = OrderGroup()
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )
		orders.append( Order( order_group, e.timestamp_index, e.symbol, shares_to_transact, "BUY" ) )
		orders.append( Order( order_group, sell_timestamp_index, e.symbol, shares_to_transact, "SELL" ) )

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders

def convert_events_to_orders_2( events, ldt_timestamps, trading_days_to_sell_delta = 5, dollar_amount_to_transact = 10000 ):
	orders = []
	for e in events:
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )
		shares_to_transact = math.floor( dollar_amount_to_transact / e.price )
		order_group = OrderGroup()
		orders.append( Order( order_group, e.timestamp_index, e.symbol, shares_to_transact, "BUY" ) )
		orders.append( Order( order_group, sell_timestamp_index, e.symbol, shares_to_transact, "SELL" ) )

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders

def convert_events_to_orders( events, ldt_timestamps, min_percentage_increase = 0.05, trading_days_to_sell_delta = 1, dollar_amount_to_transact = 10000 ):
	orders = []
	for e in events:
		sell_timestamp_index = min( e.timestamp_index + trading_days_to_sell_delta, len( ldt_timestamps ) - 1 )
		shares_to_transact = math.floor( dollar_amount_to_transact / e.price )
		order_group = OrderGroup()
		orders.append( Order( order_group, e.timestamp_index, e.symbol, shares_to_transact, "BUY" ) )
		orders.append( MinimumPriceTargetOrder( order_group, sell_timestamp_index, e.symbol, shares_to_transact, "SELL", e.price * ( 1 + min_percentage_increase ) ) )

	sorted_orders = sorted( orders, key = lambda o: o.timestamp_index )
	return sorted_orders