import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
import pandas as pd
from ..create_data_map import create_dataframe_from_common_values

class TestCreateDataFrameFromCommonValues(unittest.TestCase):

    def test_basic_functionality(self):
        data = {'test_field': [['apple', 5], ['banana', 3], ['cherry', 2]]}
        df = create_dataframe_from_common_values('test_field', data)
        self.assertEqual(df['MostCommonValues1'].iloc[0], 'apple')
        self.assertEqual(df['MostCommonValues1Count'].iloc[0], 5)
        self.assertEqual(df['MostCommonValues2'].iloc[0], 'banana')
        self.assertEqual(df['MostCommonValues2Count'].iloc[0], 3)
        self.assertEqual(df['MostCommonValues3'].iloc[0], 'cherry')
        self.assertEqual(df['MostCommonValues3Count'].iloc[0], 2)

    def test_less_than_three_values(self):
        data = {'test_field': [['apple', 5], ['banana', 3]]}
        df = create_dataframe_from_common_values('test_field', data)
        self.assertEqual(df['MostCommonValues1'].iloc[0], 'apple')
        self.assertEqual(df['MostCommonValues1Count'].iloc[0], 5)
        self.assertEqual(df['MostCommonValues2'].iloc[0], 'banana')
        self.assertEqual(df['MostCommonValues2Count'].iloc[0], 3)
        self.assertTrue(pd.isnull(df['MostCommonValues3'].iloc[0]))
        self.assertTrue(pd.isnull(df['MostCommonValues3Count'].iloc[0]))

    def test_more_than_three_values(self):
        data = {'test_field': [['apple', 5], ['banana', 3], ['cherry', 2], ['date', 1]]}
        df = create_dataframe_from_common_values('test_field', data)
        self.assertEqual(df['MostCommonValues1'].iloc[0], 'apple')
        self.assertEqual(df['MostCommonValues1Count'].iloc[0], 5)
        self.assertEqual(df['MostCommonValues2'].iloc[0], 'banana')
        self.assertEqual(df['MostCommonValues2Count'].iloc[0], 3)
        self.assertEqual(df['MostCommonValues3'].iloc[0], 'cherry')
        self.assertEqual(df['MostCommonValues3Count'].iloc[0], 2)
        # Ensure date is not in the dataframe
        self.assertNotIn('date', df.values)

    def test_field_not_in_data(self):
        data = {'another_field': [['apple', 5], ['banana', 3], ['cherry', 2]]}
        df = create_dataframe_from_common_values('test_field', data)
        self.assertTrue(df.empty)

if __name__ == '__main__':
    unittest.main()
