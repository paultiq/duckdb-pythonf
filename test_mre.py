import pandas as pd


def test_chained_assignment_warning():
    """This should trigger the ChainedAssignmentError warning we're trying to suppress"""
    df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
    
    # This chained assignment triggers the warning
    df['A'][0] = 999
    
    assert df['A'][0] == 999