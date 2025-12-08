"""
FBref Premier League Scraper - Selenium Version with Fixed Parsing
Works around 403 errors by using a real browser

Installation:
pip install selenium beautifulsoup4 webdriver-manager
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time
from pathlib import Path
import random

class FBrefSeleniumScraper:
    def __init__(self, output_folder):
        self.base_url = "https://fbref.com"
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.driver = None
        
    def init_driver(self):
        """Initialize Chrome driver"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        print("Initializing Chrome driver...")
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def close_driver(self):
        """Close browser"""
        if self.driver:
            self.driver.quit()
    
    def get_season_url(self, season_start_year):
        """Get Premier League URL for a specific season"""
        season_str = f"{season_start_year}-{season_start_year + 1}"
        return f"{self.base_url}/en/comps/9/{season_str}/{season_str}-Premier-League-Stats"
    
    def get_player_stats_url(self, season_start_year):
        """Get player stats URL"""
        season_str = f"{season_start_year}-{season_start_year + 1}"
        return f"{self.base_url}/en/comps/9/{season_str}/stats/{season_str}-Premier-League-Stats"
    
    def random_delay(self, min_sec=2, max_sec=4):
        """Random delay to mimic human behavior"""
        time.sleep(random.uniform(min_sec, max_sec))
    
    def fetch_page(self, url):
        """Fetch page with Selenium and return BeautifulSoup"""
        try:
            self.driver.get(url)
            self.random_delay(3, 5)
            
            # Wait for tables to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
            
            return BeautifulSoup(self.driver.page_source, 'html.parser')
            
        except Exception as e:
            print(f"  ✗ Error fetching {url}: {e}")
            return None
    
    def parse_league_table(self, soup, table_type="overall"):
        """Parse league table (overall, home, or away)"""
        data = []
        
        # Find the correct table
        table = None
        all_tables = soup.find_all('table')
        
        for t in all_tables:
            table_id = t.get('id', '')
            if table_type == "overall" and 'overall' in table_id.lower():
                table = t
                break
            elif table_type == "home" and 'home' in table_id.lower() and 'overall' not in table_id.lower():
                table = t
                break
            elif table_type == "away" and 'away' in table_id.lower():
                table = t
                break
        
        if not table:
            print(f"  ⚠ {table_type.capitalize()} table not found")
            return data
        
        # Extract headers
        thead = table.find('thead')
        if not thead:
            print(f"  ⚠ No thead in {table_type} table")
            return data
        
        header_rows = thead.find_all('tr')
        headers = []
        
        if header_rows:
            for th in header_rows[-1].find_all(['th', 'td']):
                header_text = th.get_text(strip=True)
                if not header_text:
                    header_text = th.get('data-stat', th.get('aria-label', 'unknown'))
                headers.append(header_text)
        
        if not headers:
            print(f"  ⚠ No headers in {table_type} table")
            return data
        
        # Extract rows
        tbody = table.find('tbody')
        if not tbody:
            print(f"  ⚠ No tbody in {table_type} table")
            return data
        
        for row in tbody.find_all('tr'):
            if 'thead' in row.get('class', []) or 'spacer' in row.get('class', []):
                continue
            
            cells = row.find_all(['th', 'td'])
            if not cells:
                continue
            
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.get_text(strip=True)
            
            if row_data and len(row_data) > 1:
                data.append(row_data)
        
        print(f"  ✓ {table_type.capitalize()}: {len(data)} rows")
        return data
    
    def parse_squad_stats(self, soup):
        """Parse squad standard stats"""
        data = []
        
        table = soup.find('table', {'id': lambda x: x and 'stats_squads_standard' in str(x)})
        
        if not table:
            print(f"  ⚠ Squad stats table not found")
            return data
        
        thead = table.find('thead')
        if not thead:
            return data
        
        header_rows = thead.find_all('tr')
        headers = []
        
        if header_rows:
            for th in header_rows[-1].find_all(['th', 'td']):
                header_text = th.get_text(strip=True)
                if not header_text:
                    header_text = th.get('data-stat', 'unknown')
                if header_text in headers:
                    header_text = f"{header_text}_{headers.count(header_text)}"
                headers.append(header_text)
        
        if not headers:
            return data
        
        tbody = table.find('tbody')
        if not tbody:
            return data
        
        for row in tbody.find_all('tr'):
            if 'thead' in row.get('class', []) or 'spacer' in row.get('class', []):
                continue
            
            cells = row.find_all(['th', 'td'])
            if not cells:
                continue
            
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.get_text(strip=True)
            
            if row_data and len(row_data) > 1:
                data.append(row_data)
        
        print(f"  ✓ Squad stats: {len(data)} rows")
        return data
    
    def parse_player_stats(self, soup):
        """Parse player standard stats"""
        data = []
        
        table = soup.find('table', {'id': lambda x: x and 'stats_standard' in str(x)})
        
        if not table:
            print(f"  ⚠ Player stats table not found")
            return data
        
        thead = table.find('thead')
        if not thead:
            return data
        
        header_rows = thead.find_all('tr')
        headers = []
        
        if header_rows:
            for th in header_rows[-1].find_all(['th', 'td']):
                header_text = th.get_text(strip=True)
                if not header_text:
                    header_text = th.get('data-stat', 'unknown')
                if header_text in headers:
                    header_text = f"{header_text}_{headers.count(header_text)}"
                headers.append(header_text)
        
        if not headers:
            return data
        
        tbody = table.find('tbody')
        if not tbody:
            return data
        
        for row in tbody.find_all('tr'):
            if 'thead' in row.get('class', []) or 'spacer' in row.get('class', []):
                continue
            
            cells = row.find_all(['th', 'td'])
            if not cells:
                continue
            
            row_data = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_data[headers[i]] = cell.get_text(strip=True)
            
            if row_data and len(row_data) > 1:
                data.append(row_data)
        
        print(f"  ✓ Player stats: {len(data)} rows")
        return data
    
    def scrape_season(self, season_start_year):
        """Scrape all data for one season"""
        season_str = f"{season_start_year}-{season_start_year + 1}"
        print(f"\n{'='*60}")
        print(f"SEASON {season_str}")
        print(f"{'='*60}")
        
        try:
            # Fetch main page
            main_url = self.get_season_url(season_start_year)
            print(f"Fetching main page...")
            soup_main = self.fetch_page(main_url)
            
            if not soup_main:
                print(f"✗ Failed to fetch main page")
                return
            
            # Parse league tables
            print("Parsing league tables...")
            league_table_overall = self.parse_league_table(soup_main, "overall")
            league_table_home = self.parse_league_table(soup_main, "home")
            league_table_away = self.parse_league_table(soup_main, "away")
            
            # Parse squad stats
            print("Parsing squad stats...")
            squad_stats = self.parse_squad_stats(soup_main)
            
            # Fetch player stats page
            player_url = self.get_player_stats_url(season_start_year)
            print(f"Fetching player stats page...")
            soup_players = self.fetch_page(player_url)
            
            player_stats = []
            if soup_players:
                print("Parsing player stats...")
                player_stats = self.parse_player_stats(soup_players)
            else:
                print("  ⚠ Could not fetch player stats")
            
            # Save files
            season_folder = self.output_folder / season_str
            season_folder.mkdir(exist_ok=True)
            
            print(f"\nSaving files...")
            
            with open(season_folder / "league_table_overall.json", 'w', encoding='utf-8') as f:
                json.dump(league_table_overall, f, indent=2, ensure_ascii=False)
            print(f"  ✓ league_table_overall.json ({len(league_table_overall)} rows)")
            
            with open(season_folder / "league_table_home.json", 'w', encoding='utf-8') as f:
                json.dump(league_table_home, f, indent=2, ensure_ascii=False)
            print(f"  ✓ league_table_home.json ({len(league_table_home)} rows)")
            
            with open(season_folder / "league_table_away.json", 'w', encoding='utf-8') as f:
                json.dump(league_table_away, f, indent=2, ensure_ascii=False)
            print(f"  ✓ league_table_away.json ({len(league_table_away)} rows)")
            
            with open(season_folder / "squad_stats.json", 'w', encoding='utf-8') as f:
                json.dump(squad_stats, f, indent=2, ensure_ascii=False)
            print(f"  ✓ squad_stats.json ({len(squad_stats)} rows)")
            
            with open(season_folder / "player_stats.json", 'w', encoding='utf-8') as f:
                json.dump(player_stats, f, indent=2, ensure_ascii=False)
            print(f"  ✓ player_stats.json ({len(player_stats)} rows)")
            
            print(f"\n✅ Season {season_str} completed!")
            
        except Exception as e:
            print(f"✗ Error scraping season {season_str}: {e}")
    
    def scrape_all_seasons(self, start_year=2014, end_year=2024):
        """Scrape all seasons"""
        print(f"\n{'='*60}")
        print(f"FBref Premier League Scraper - Selenium")
        print(f"{'='*60}")
        print(f"Output: {self.output_folder}")
        print(f"Seasons: {start_year}-{start_year+1} to {end_year}-{end_year+1}")
        print(f"{'='*60}")
        
        self.init_driver()
        
        try:
            for year in range(start_year, end_year + 1):
                self.scrape_season(year)
                if year < end_year:
                    print(f"\nWaiting before next season...")
                    self.random_delay(4, 7)
        finally:
            self.close_driver()
        
        print(f"\n{'='*60}")
        print(f"✅ ALL COMPLETED!")
        print(f"📁 Location: {self.output_folder}")
        print(f"{'='*60}\n")

def main():
    output_folder = r"C:\Users\dell\OneDrive\Desktop\DW JasonFiles(2)"
    
    scraper = FBrefSeleniumScraper(output_folder)
    scraper.scrape_all_seasons(2014, 2024)

if __name__ == "__main__":
    main()