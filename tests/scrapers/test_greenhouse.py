import unittest
import json
from unittest.mock import patch, MagicMock
from src.scrapers.greenhouse import scrape_greenhouse_jobs

class TestGreenhouseScraper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Load the sample JSON data once for all tests in this class."""
        with open('tests/fixtures/stripe_sample.json', 'r') as f:
            cls.mock_api_data = json.load(f)

    @patch('requests.get')
    def test_scrape_greenhouse_jobs_success(self, mock_get):
        """
        Tests that the scraper correctly parses a successful API response and
        adds the LinkedIn URLS.
        """
        # Configure the mock to return a successful response with our sample JSON
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_data
        mock_get.return_value = mock_response

        # Define a simple keyword filter for the test
        test_keywords = ['new grad', 'software engineer']

        # Call the function we are testing
        jobs = scrape_greenhouse_jobs("Stripe", "stripe", test_keywords)

        # --- Assertions: Check if the function behaved as expected ---
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]['Company'], 'Stripe')
        self.assertEqual(jobs[0]['Title'], 'New Grad Software Engineer')
        
        # --- NEW TESTS TO VERIFY THE LINKS ---
        # 1. Check that the new keys exist in the output
        self.assertIn('Alumni_Search_URL', jobs[0])
        self.assertIn('Role_Search_URL', jobs[0])

        # 2. Check that the content of the URLs is correct
        self.assertIn('Stripe', jobs[0]['Alumni_Search_URL'])
        self.assertIn('McMaster', jobs[0]['Alumni_Search_URL'])
        self.assertIn('New+Grad+Software+Engineer', jobs[0]['Role_Search_URL'])


if __name__ == '__main__':
    unittest.main()