#!/usr/bin/env python3
"""
Options Trading Simulator - JSON Configuration Runner

This script allows you to run the options trading simulator using pre-configured
JSON files instead of interactive user input.
"""

import json
import os
import time
from pathlib import Path
from simulator import Simulator


def list_config_files():
    """List all JSON configuration files in the config_templates directory."""
    config_dir = Path("config_templates")
    
    if not config_dir.exists():
        print("Config templates directory not found!")
        return []
    
    json_files = list(config_dir.glob("*.json"))
    return json_files


def load_config_file(file_path):
    """Load and validate a JSON configuration file."""
    try:
        with open(file_path, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        required_fields = ['simulation_config', 'strategies']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate simulation_config
        sim_config = config['simulation_config']
        required_sim_fields = ['start_date', 'end_date', 'starting_balance']
        for field in required_sim_fields:
            if field not in sim_config:
                raise ValueError(f"Missing required simulation config field: {field}")
        
        # Validate strategies
        if not isinstance(config['strategies'], list) or len(config['strategies']) == 0:
            raise ValueError("Strategies must be a non-empty list")
        
        for i, strategy in enumerate(config['strategies']):
            required_strategy_fields = [
                'spread_type', 'conditions', 'start_time_window', 'end_time_window',
                'width', 'offset', 'stop_loss_type', 'take_profit_level',
                'max_active_positions', 'hedge'
            ]
            for field in required_strategy_fields:
                if field not in strategy:
                    raise ValueError(f"Missing required field '{field}' in strategy {i+1}")
        
        return config
        
    except FileNotFoundError:
        print(f"Error: Configuration file '{file_path}' not found.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in '{file_path}': {e}")
        return None
    except ValueError as e:
        print(f"Error: Configuration validation failed for '{file_path}': {e}")
        return None
    except Exception as e:
        print(f"Error: Unexpected error loading '{file_path}': {e}")
        return None


def display_config_summary(config):
    """Display a summary of the loaded configuration."""
    sim_config = config['simulation_config']
    strategies = config['strategies']
    
    print("\n" + "="*60)
    print("CONFIGURATION SUMMARY")
    print("="*60)
    
    print(f"\nSimulation Parameters:")
    print(f"  Start Date: {sim_config['start_date']}")
    print(f"  End Date: {sim_config['end_date']}")
    print(f"  Starting Balance: ${sim_config['starting_balance']:,.2f}")
    
    print(f"\nStrategies ({len(strategies)}):")
    for i, strategy in enumerate(strategies, 1):
        print(f"\n  Strategy {i} - {strategy['spread_type'].upper()}:")
        print(f"    Time Window: {strategy['start_time_window']} - {strategy['end_time_window']}")
        print(f"    Width: {strategy['width']}, Offset: {strategy['offset']}")
        print(f"    Stop Loss: {strategy['stop_loss_type']}")
        print(f"    Take Profit: {strategy['take_profit_level']*100:.1f}%")
        print(f"    Max Positions: {strategy['max_active_positions']}")
        if strategy['hedge']:
            print(f"    Hedge: {strategy['hedge']}")
        if strategy['conditions']:
            print(f"    Conditions: {strategy['conditions']}")
    
    print("="*60)


def convert_config_to_simulator_params(config):
    """Convert JSON config format to simulator parameters format."""
    sim_config = config['simulation_config']
    
    return {
        'start_date': sim_config['start_date'],
        'end_date': sim_config['end_date'],
        'starting_balance': sim_config['starting_balance'],
        'strategies': config['strategies']
    }


def select_config_file():
    """Let user select a configuration file from available options."""
    config_files = list_config_files()
    
    if not config_files:
        print("No configuration files found in 'config_templates' directory.")
        print("\nTo create configuration files:")
        print("1. Create a 'config_templates' directory")
        print("2. Add JSON files with the required structure")
        print("3. See 'example_config.json' for reference")
        return None
    
    print("\n" + "="*60)
    print("AVAILABLE CONFIGURATION FILES")
    print("="*60)
    
    for i, file_path in enumerate(config_files, 1):
        print(f"{i}. {file_path.name}")
    
    print("="*60)
    
    while True:
        try:
            choice = input(f"\nSelect a configuration file (1-{len(config_files)}): ").strip()
            
            if not choice:
                print("Please enter a number.")
                continue
                
            choice_num = int(choice)
            if 1 <= choice_num <= len(config_files):
                selected_file = config_files[choice_num - 1]
                print(f"Selected: {selected_file.name}")
                return selected_file
            else:
                print(f"Please enter a number between 1 and {len(config_files)}.")
        except ValueError:
            print("Please enter a valid number.")
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            return None


def main():
    """Main entry point for JSON-based simulation runner."""
    print("\n\n#######################################################")
    print("# ------- OPTIONS TRADING SIMULATOR (JSON MODE) ------- #")
    print("#######################################################\n")
    
    # Select configuration file
    config_file = select_config_file()
    if not config_file:
        return
    
    # Load configuration
    print(f"\nLoading configuration from: {config_file}")
    config = load_config_file(config_file)
    if not config:
        return
    
    # Display configuration summary
    display_config_summary(config)
    
    # Confirm execution
    while True:
        confirm = input("\nDo you want to run the simulation with this configuration? (y/n): ").strip().lower()
        if confirm in ['y', 'yes']:
            break
        elif confirm in ['n', 'no']:
            print("Simulation cancelled.")
            return
        else:
            print("Please enter 'y' or 'n'.")
    
    # Convert config and run simulation
    params = convert_config_to_simulator_params(config)
    
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
        
        # Display summary
        print(f"\nFinal Results:")
        print(f"Starting Balance: ${params['starting_balance']:.2f}")
        final_balance = params['starting_balance'] + overall_stats['total_profit']
        print(f"Final Balance: ${final_balance:.2f}")
        profit = overall_stats['total_profit']
        roi = ((profit / params['starting_balance']) * 100) if params['starting_balance'] > 0 else 0
        print(f"Net Profit/Loss: ${profit:.2f} ({roi:.2f}%)")
        
        end_time = time.time()
        print(f"\nSimulation completed in {end_time - start_time:.2f} seconds")
        
    else:
        print("Simulation failed or produced no results.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSimulation interrupted by user.")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()