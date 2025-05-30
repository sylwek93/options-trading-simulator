import polars as pl
from datetime import datetime, timedelta
from database import query_with_conditions
from spread import PutCreditSpread, CallCreditSpread

class Simulator:
    def __init__(self, params):
        self.start_date = params['start_date']
        self.end_date = params['end_date']
        self.starting_balance = params['starting_balance']
        self.strategies = params['strategies']

        self.trading_start_time = '15:30:00'
        self.trading_end_time = '22:00:00'

        self.slippage = 0.05
        self.commission = 1.5
        
        self.trades = pl.DataFrame({
            'spread_type': pl.Series([], dtype=pl.Utf8),
            'width': pl.Series([], dtype=pl.Float64),
            'offset': pl.Series([], dtype=pl.Float64),
            'stop_loss_type': pl.Series([], dtype=pl.Utf8),
            'take_profit_level': pl.Series([], dtype=pl.Float64),
            'strikes': pl.Series([], dtype=pl.List(pl.Float64)),
            'max_loss': pl.Series([], dtype=pl.Float64),
            'max_profit': pl.Series([], dtype=pl.Float64),
            'break_even_level': pl.Series([], dtype=pl.Float64),
            'break_even_time': pl.Series([], dtype=pl.Utf8),
            'entry_time': pl.Series([], dtype=pl.Utf8),
            'entry_price': pl.Series([], dtype=pl.Float64),
            'exit_time': pl.Series([], dtype=pl.Utf8),
            'exit_price': pl.Series([], dtype=pl.Float64),
            'pnl': pl.Series([], dtype=pl.Float64),
            'outcome': pl.Series([], dtype=pl.Utf8),
            'current_status': pl.Series([], dtype=pl.Utf8)
        })
        
        # Calculate business dates during initialization
        self.business_dates = self.calculate_business_days()
        self.total_business_days = len(self.business_dates)

    def calculate_business_days(self):
        """Calculate list of business dates between start_date and end_date (excluding weekends)"""
        if isinstance(self.start_date, str):
            start = datetime.strptime(self.start_date, '%Y-%m-%d')
        else:
            start = self.start_date
            
        if isinstance(self.end_date, str):
            end = datetime.strptime(self.end_date, '%Y-%m-%d')
        else:
            end = self.end_date
            
        business_dates = []
        current_date = start
        
        while current_date <= end:
            # Monday = 0, Sunday = 6
            if current_date.weekday() < 5:  # Monday to Friday
                business_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
            
        return business_dates
    
 
    def run_simulator(self):
        """Run the trading simulator for each business day and trading minute"""

        strategies_data = {}
        spx_prices = query_with_conditions(self.start_date, self.end_date, self.trading_start_time, self.trading_end_time, '')

        print(f"Processing {self.total_business_days} trading days")

        for strategy in self.strategies:
            strategy_name = strategy['spread_type']
            strategy_conditions = strategy['conditions']
            start_time_window = strategy['start_time_window']
            end_time_window = strategy['end_time_window']
            max_active_positions = strategy['max_active_positions']
            width = strategy['width']
            offset = strategy['offset']
            stop_loss_type = strategy['stop_loss_type']
            take_profit_level = strategy['take_profit_level']
            
            # Query data for this strategy
            entries_df = query_with_conditions(self.start_date, self.end_date, start_time_window, end_time_window, strategy_conditions)
            
            # Store all strategy parameters and data in the dictionary
            strategies_data[strategy_name] = {
                'entries': entries_df,
                'max_active_positions': max_active_positions,
                'width': width,
                'offset': offset,
                'stop_loss_type': stop_loss_type,
                'take_profit_level': take_profit_level,
                'active_positions': 0
            }

        for date in self.business_dates:
            # Create datetime objects for trading start and end times
            start_time = datetime.strptime(f"{date} {self.trading_start_time}", '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(f"{date} {self.trading_end_time}", '%Y-%m-%d %H:%M:%S')

            used_call_strikes = set()
            used_put_strikes = set()
            
            # Iterate through each minute during trading hours
            current_time = start_time
            while current_time <= end_time:

                current_date_str = current_time.strftime('%Y-%m-%d')
                current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')

                #print(f"Pricessing: {current_time_str}")
                
                current_price_data = spx_prices.filter(pl.col('date_time') == current_time_str)
                eod_price_data = spx_prices.filter(pl.col('date_time').str.contains(current_date_str)).sort('date_time')
                
                current_spx_price = None
                eod_spx_price = None
                tail_price_data = None

                if not eod_price_data.is_empty():
                    tail_price_data = eod_price_data.tail(1)
                    eod_spx_price = tail_price_data['spx_price'].item()
                
                if not current_price_data.is_empty():
                    current_spx_price = current_price_data['spx_price'].item()


                for strategy_name, strategy_data in strategies_data.items():

                    ################################################################################
                    # CHECK FOR NEW TRADES
                    if current_spx_price and eod_spx_price:
                        if hasattr(self, 'trades') and not self.trades.is_empty():
                            active_positions_count = self.trades.filter(
                                (pl.col('spread_type') == strategy_name) & 
                                (pl.col('current_status') == 'active')
                            ).height
                            
                            strategy_data['active_positions'] = active_positions_count
                        else:
                            strategy_data['active_positions'] = 0


                        if strategy_data['active_positions'] < strategy_data['max_active_positions']:
                            entries = strategy_data['entries']
                            matching_data = entries.filter(pl.col('date_time') == current_time_str)
                            
                            if len(matching_data) > 0:

                                if strategy_name == 'call_spread':
                                    call = CallCreditSpread()
                                    new_call = call.get_spread_data(current_spx_price, eod_spx_price, current_time, strategy_data, self.slippage, self.commission)

                                    if not new_call.is_empty():
                                        # Check if strikes are already in use
                                        call_strikes = new_call['strikes'].item()
                                        strikes_in_use = any(strike in used_call_strikes for strike in call_strikes)
                                        
                                        if not strikes_in_use:
                                            # Add strikes to used set
                                            for strike in call_strikes:
                                                used_call_strikes.add(strike)
                                            self.trades = pl.concat([self.trades, new_call])

                                elif strategy_name == 'put_spread':
                                    put = PutCreditSpread()
                                    new_put = put.get_spread_data(current_spx_price, eod_spx_price, current_time, strategy_data, self.slippage, self.commission)

                                    if not new_put.is_empty():
                                        # Check if strikes are already in use
                                        put_strikes = new_put['strikes'].item()
                                        strikes_in_use = any(strike in used_put_strikes for strike in put_strikes)
                                        
                                        if not strikes_in_use:
                                            # Add strikes to used set
                                            for strike in put_strikes:
                                                used_put_strikes.add(strike)
                                            self.trades = pl.concat([self.trades, new_put])

                    ################################################################################
                    # UPDATE ACTIVE TRADES STATUS

                    if hasattr(self, 'trades') and not self.trades.is_empty():

                        close_mask = (pl.col('exit_time') <= current_time_str) & (
                            pl.col('current_status') == 'active')

                        if self.trades.filter(close_mask).height > 0:
                            # Remove strikes from used_put_strikes for closing positions
                            closing_trades = self.trades.filter(close_mask)
                            for row in closing_trades.iter_rows(named=True):
                                if row['spread_type'] == 'put_spread':
                                    for strike in row['strikes']:
                                        used_put_strikes.discard(strike)
                                elif row['spread_type'] == 'call_spread':
                                    for strike in row['strikes']:
                                        used_call_strikes.discard(strike)
                            
                            self.trades = self.trades.with_columns(
                                pl.when(close_mask)
                                .then(pl.lit('close'))
                                .otherwise(pl.col('current_status'))
                                .alias('current_status')
                            )

                    ################################################################################
                
                # Move to next minute
                current_time += timedelta(minutes=1)
        
        # self.trades summary here

        # Convert nested list column to semicolon separated string before writing to CSV
        #csv_safe_trades = self.trades.with_columns(
        #    pl.col('strikes').map_elements(lambda x: ";".join(str(strike) for strike in x)).alias('strikes')
        #)
        #csv_safe_trades.write_csv('tests/test_spread.csv')
        #print(self.trades)




#test
'''
strategies_data = [{
    'spread_type': 'put_spread',
    'conditions': '',
    'start_time_window': '15:31',
    'end_time_window': '21:30',
    'max_active_positions': 1,
    'width': 10,
    'offset': 0,
    'stop_loss_type': 'bep',
    'take_profit_level': 0.01
}]

sim = Simulator({
    'start_date': '2025-05-01',
    'end_date': '2025-05-02',
    'starting_balance': 10000,
    'strategies': strategies_data

})

sim.run_simulator()
'''
