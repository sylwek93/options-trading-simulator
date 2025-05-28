import polars as pl
from datetime import datetime, timedelta
from database import query_with_conditions, query_option_chain


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
        
        self.active_trades = pl.DataFrame({
            'spread_type': pl.Series([], dtype=pl.Utf8),
            'width': pl.Series([], dtype=pl.Float64),
            'offset': pl.Series([], dtype=pl.Float64),
            'stop_loss_type': pl.Series([], dtype=pl.Utf8),
            'take_profit_level': pl.Series([], dtype=pl.Float64),
            'strikes': pl.Series([], dtype=pl.List(pl.Float64)),
            'max_loss': pl.Series([], dtype=pl.Float64),
            'max_profit': pl.Series([], dtype=pl.Float64),
            'break_even': pl.Series([], dtype=pl.List(pl.Float64)),
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

        print(f"Processing {self.total_business_days} trading days")

        for strategy in self.strategies:
            
            strategy_name = strategy['spread_type']
            strategy_conditions = strategy['conditions']
            start_time_widow = ['start_time_widow']
            end_time_window = ['end_time_window']

            strategies_data[strategy_name] = query_with_conditions(self.start_date, self.end_date, start_time_widow, end_time_window, strategy_conditions)


        for date in self.business_dates:
            # Create datetime objects for trading start and end times
            start_time = datetime.strptime(f"{date} {self.trading_start_time}", '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(f"{date} {self.trading_end_time}", '%Y-%m-%d %H:%M:%S')
            
            # Iterate through each minute during trading hours
            current_time = start_time
            while current_time <= end_time:
                # Print current timestamp
                print(current_time)
                
                # TODO: Add trading logic here
                # - Check for entry conditions
                # - Monitor existing positions
                # - Execute trades
                # - Update portfolio
                
                # Move to next minute
                current_time += timedelta(minutes=1)


#test
sim = Simulator({
    'start_date': '2025-05-01',
    'end_date': '2025-05-24',
    'starting_balance': 10000
})

sim.run_simulator()
