#!/usr/bin/env python3
"""
A simple Python script to be called by the Airflow DAG.
"""

import datetime
import random


def main():
    """Main function that performs a simple task."""
    current_time = datetime.datetime.now()
    random_number = random.randint(1, 100)
    
    print(f"Script executed successfully at {current_time}")
    print(f"Generated random number: {random_number}")
    
    # Simulating some data processing
    result = sum(range(random_number))
    print(f"Sum of numbers from 0 to {random_number-1}: {result}")
    
    return result


if __name__ == "__main__":
    main()
