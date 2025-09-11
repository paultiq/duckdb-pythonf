#!/usr/bin/env python3
"""
Minimal reproducible example for pandas ChainedAssignmentError warning filtering
"""
import warnings
import pandas as pd


def test_chained_assignment_warning():
    """Test that reproduces the ChainedAssignmentError warning from pandas nightly"""
    # This should trigger the warning we're trying to suppress
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    
    # This chained assignment should trigger the ChainedAssignmentError warning
    df['A'][0] = 999
    
    print("Test completed - if you see a FutureWarning above, the filter isn't working")


if __name__ == "__main__":
    print("=== Running without any warning filters ===")
    test_chained_assignment_warning()
    
    print("\n=== Running with PYTHONWARNINGS filter ===")
    import os
    os.environ['PYTHONWARNINGS'] = 'ignore:ChainedAssignmentError.*:FutureWarning'
    test_chained_assignment_warning()
    
    print("\n=== Running with warnings.filterwarnings ===")
    warnings.filterwarnings('ignore', message='ChainedAssignmentError.*', category=FutureWarning)
    test_chained_assignment_warning()