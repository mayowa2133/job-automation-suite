import unittest
import os
import pandas as pd
import tempfile
from unittest.mock import patch
from src import main

class TestMainWorkflow(unittest.TestCase):

    @patch('src.main.scrape_greenhouse_jobs')
    @patch('src.main.open')
    def test_main_script_workflow(self, mock_open, mock_scrape_greenhouse):
        """
        Tests the full end-to-end workflow of the main.py script.
        """
        # --- Setup Mocks ---
        # 1. Mock the config file reading
        mock_open.return_value.__enter__.return_value.read.return_value = '''
        {
            "greenhouse": { "TestCorp": "testcorp" }
        }
        '''
        
        # 2. Mock the return value of the scraper function
        mock_scrape_greenhouse.return_value = [
            {'Company': 'TestCorp', 'Title': 'New Grad Test Engineer', 'URL': 'http://test.com', 'Location': 'Testville'}
        ]

        # --- Run Test in a Temporary Directory ---
        with tempfile.TemporaryDirectory() as tmpdir:
            # Tell the main script to save its output here
            main.OUTPUT_DIR = tmpdir + '/'

            # Run the main function's logic
            main.run_scrapers()

            # --- Assertions ---
            # 1. Check that the scraper was called correctly
            mock_scrape_greenhouse.assert_called_once_with(
                "TestCorp", "testcorp", main.KEYWORD_FILTERS
            )

            # 2. Check that the CSV file was created and contains the correct data
            output_files = os.listdir(tmpdir)
            self.assertEqual(len(output_files), 1)
            self.assertTrue(output_files[0].startswith('job_postings_'))

            df = pd.read_csv(os.path.join(tmpdir, output_files[0]))
            self.assertEqual(len(df), 1)
            self.assertEqual(df.iloc[0]['Company'], 'TestCorp')
            self.assertEqual(df.iloc[0]['Title'], 'New Grad Test Engineer')

# Note: We need to slightly modify main.py to make its core logic runnable
# from a test. Wrap the main logic in a function like `run_scrapers()`.
# Then, the `if __name__ == "__main__":` block will simply call `run_scrapers()`.