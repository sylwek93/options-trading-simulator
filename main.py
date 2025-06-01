import re
import time
import datetime
from simulator import Simulator

# Constants
TIME_PATTERN = re.compile(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$')
DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
TRADING_DAY_START = '15:30:00'
TRADING_DAY_END = '22:00:00'
DEFAULT_BALANCE = 10000.0


def validate_time_format(time_str):
    """Validate time string in HH:MM format."""
    return bool(TIME_PATTERN.match(time_str))


def validate_date_format(date_str):
    """Validate date string in YYYY-MM-DD format."""
    if not DATE_PATTERN.match(date_str):
        return False
    try:
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False


def get_validated_input(prompt, validator_func=None, default=None, allowed_values=None, converter_func=None, error_message=None):
    """Generic input validation function to reduce code duplication."""
    while True:
        user_input = input(prompt)

        # Use default if input empty and default provided
        if not user_input and default is not None:
            print(f"Using default value: {default}")
            return default

        # Check against allowed values if specified
        if allowed_values and user_input not in allowed_values:
            print(f"Invalid input. Please choose from: {', '.join(map(str, allowed_values))}")
            continue

        # Validate with custom function if provided
        if validator_func and not validator_func(user_input):
            print(error_message or "Invalid input. Please try again.")
            continue

        # Convert value if converter provided
        if converter_func:
            try:
                return converter_func(user_input)
            except (ValueError, TypeError):
                print(f"Invalid input. Cannot convert to required type.")
                continue

        return user_input


def validate_positive_float(value):
    """Validate if value is a positive float."""
    try:
        return float(value) > 0
    except ValueError:
        return False


def get_spread_parameters(spread_type):
    """Get parameters for a specific spread type."""
    print(f"\nParameters for {spread_type}:")

    # Get time window with validation
    start_time_window = get_validated_input(
        "Enter start time (HH:MM): ",
        validator_func=validate_time_format,
        default=TRADING_DAY_START[:5],
        error_message="Invalid time format. Please use HH:MM format."
    )

    end_time_window = get_validated_input(
        "Enter end time (HH:MM): ",
        validator_func=validate_time_format,
        default=TRADING_DAY_END[:5],
        error_message="Invalid time format. Please use HH:MM format."
    )

    width = get_validated_input(
        "Enter spread width: ",
        validator_func=lambda x: x.replace('.', '').isdigit() and float(x) > 0,
        default="10",
        converter_func=float,
        error_message="Please enter a positive number."
    )

    offset = get_validated_input(
        "Enter spread offset: ",
        validator_func=lambda x: x.lstrip('-').replace('.', '').isdigit(),
        default="0",
        converter_func=float,
        error_message="Please enter a valid number (can be negative)."
    )

    stop_loss_type = get_validated_input(
        "Enter spread stop loss type [bep, expire]: ",
        allowed_values=["bep", "expire"],
        default="bep"
    )

    take_profit_level = get_validated_input(
        "Enter spread take profit level (e.g., 0.01 for 1%): ",
        validator_func=lambda x: x.replace('.', '').isdigit() and 0 < float(x) <= 1,
        default="0.01",
        converter_func=float,
        error_message="Please enter a number between 0 and 1 (e.g., 0.01 for 1%)."
    )

    max_active_positions = get_validated_input(
        "Enter maximum active positions: ",
        validator_func=lambda x: x.isdigit() and int(x) > 0,
        converter_func=int,
        default=1,
        error_message="Please enter a positive integer."
    )

    hedge = get_validated_input(
        "Enter hedge (leave blank or 'box'): ",
        allowed_values=["", "box"],
        default=""
    )

    print("\nEnter market conditions (examples):")
    print("\nAvailable metrics:")
    print("Leave blank for no conditions, or enter custom SQL-like conditions")
    market_conditions = input("\nEnter your market conditions (leave blank for none): ")

    return {
        'spread_type': spread_type,
        'conditions': market_conditions,
        'start_time_window': start_time_window,
        'end_time_window': end_time_window,
        'width': width,
        'offset': offset,
        'stop_loss_type': stop_loss_type,
        'take_profit_level': take_profit_level,
        'max_active_positions': max_active_positions,
        'hedge': hedge
    }


def get_user_input():
    """Get backtest parameters from user input with improved validation."""
    print("Starting user input collection")
    print("====================================")

    # Get spread types first
    print("\nSpread Types")
    print("Enter comma-separated spread types to test")
    spread_types_input = get_validated_input(
        "Enter spread types [call_spread, put_spread]: ",
        validator_func=lambda x: all(s.strip() in ["call_spread", "put_spread"] for s in x.split(',')),
        default="put_spread",
        error_message="Invalid spread type. Please use comma-separated values from: call_spread, put_spread"
    )

    # Parse spread types
    spread_types = [s.strip() for s in spread_types_input.split(',')]

    # Get parameters for each spread type
    strategies = []
    for spread_type in spread_types:
        strategies.append(get_spread_parameters(spread_type))

    # Get account settings
    print("\nAccount Settings")

    # Get date range with improved validation
    start_date = get_validated_input(
        "Enter start date (YYYY-MM-DD): ",
        validator_func=validate_date_format,
        default="2025-05-01",
        error_message="Invalid date format. Please use YYYY-MM-DD format."
    )

    end_date = get_validated_input(
        "Enter end date (YYYY-MM-DD): ",
        validator_func=validate_date_format,
        default="2025-05-02",
        error_message="Invalid date format. Please use YYYY-MM-DD format."
    )

    # Get starting balance with validation
    starting_balance = get_validated_input(
        "Enter starting account balance ($): ",
        validator_func=validate_positive_float,
        converter_func=float,
        default=DEFAULT_BALANCE,
        error_message="Please enter a positive number."
    )

    # Combine all parameters
    params = {
        'start_date': start_date,
        'end_date': end_date,
        'starting_balance': starting_balance,
        'strategies': strategies
    }

    print(f"\n\nCollected user parameters: {params}\n\n")
    return params


def main():
    print("\n\n#######################################################")
    print("# ------------ OPTIONS TRADING SIMULATOR ------------ #")
    print("#######################################################\n\n")

    # Get parameters from user
    params = get_user_input()

    print("\nRunning backtest simulation...")
    print(f"Starting backtest from {params['start_date']} to {params['end_date']}")
    start_time = time.time()

    # Create and run simulator
    simulator = Simulator(params)

    print(f"Processing {simulator.total_business_days} trading days...")

    # Run simulation
    results = simulator.run_simulator()

    if results:
        print("\nBacktest completed successfully!")
        
        # Results are already analyzed in the simulator
        overall_stats = results['overall_stats']
        spread_stats = results['spread_stats']
        
        # Display summary
        print(f"\nFinal Results:")
        print(f"Starting Balance: ${params['starting_balance']:.2f}")
        final_balance = params['starting_balance'] + overall_stats['total_profit']
        print(f"Final Balance: ${final_balance:.2f}")
        profit = overall_stats['total_profit']
        roi = ((profit / params['starting_balance']) * 100) if params['starting_balance'] > 0 else 0
        print(f"Net Profit/Loss: ${profit:.2f} ({roi:.2f}%)")
        
        # Strategy breakdown
        print("\nStrategy Breakdown:")
        for row in spread_stats.iter_rows(named=True):
            print(f"Strategy {row['spread_type'].upper()}:")
            print(f"  Trades: {row['num_trades']}")
            print(f"  Win Rate: {row['win_rate']:.2f}%")
            print(f"  Total Profit: ${row['total_profit']:.2f}")
            print(f"  Average PnL: ${row['avg_pnl']:.2f}")
    else:
        print("No matching trade data found for any strategy")

    # Calculate and display elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    print(f"\nBacktest completed in {minutes:02d}:{seconds:02d}")


if __name__ == "__main__":
    main()