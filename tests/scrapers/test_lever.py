import unittest
import json
from unittest.mock import patch, MagicMock
from src.scrapers.lever import scrape_lever_jobs

class TestLeverScraper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open('tests/fixtures/atlassian_sample.json', 'r') as f:
            cls.mock_api_data = json.load(f)

    @patch('requests.get')
    def test_scrape_lever_jobs_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_data
        mock_get.return_value = mock_response

        test_keywords = ['graduate', 'engineer']
        jobs = scrape_lever_jobs("Atlassian", "atlassian", test_keywords)

        self.assertGreater(len(jobs), 0) # Check that we found at least one job
        self.assertEqual(jobs[0]['Company'], 'Atlassian')
        self.assertIn('Graduate', jobs[0]['Title'])
        self.assertIn('Alumni_Search_URL', jobs[0])