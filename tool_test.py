"""
timer_decorator.py

This script provides a reusable decorator to measure the execution time of functions.
The decorator can be applied to any function, and it will display the time taken to execute.

Usage:
    1. Import the decorator from this script.
    2. Apply the decorator to any function you want to measure.

Example:
    from timer_decorator import measure_time

    @measure_time
    def my_function():
        # Function code
        pass

The decorator will print the execution time in seconds when the function is called.
"""

import time
from functools import wraps

def measure_time(func):
    """
    Decorator to measure the execution time of a function.

    Args:
        func (callable): The function to which the decorator will be applied.

    Returns:
        callable: A wrapped function that measures execution time.

    Example:
        @measure_time
        def my_function():
            pass
    """
    @wraps(func)  # Preserves the original function's metadata
    def wrapper(*args, **kwargs):
        start_time = time.time()  # Start time
        result = func(*args, **kwargs)  # Execute the function
        end_time = time.time()  # End time
        elapsed_time = end_time - start_time  # Elapsed time
        print(f"Function '{func.__name__}' executed in {elapsed_time:.4f} seconds.")
        return result
    return wrapper

# Example usage (optional, for demonstration)
if __name__ == "__main__":
    @measure_time
    def example_function():
        """Example function to demonstrate the decorator."""
        time.sleep(2)  # Simulates an operation that takes 2 seconds
        print("Function executed!")

    # Call the example function
    example_function()