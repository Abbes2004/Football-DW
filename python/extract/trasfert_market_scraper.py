import json
import os
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime

class TransfermarktScraper:
    def __init__(self, output_folder):
        self.output_folder = output_folder
        self.driver = None
        self.setup_driver()
        self.player_cache = {}  # Cache to avoid re-scraping same players
        
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
    def setup_driver(self):
        """Setup Chrome driver with options to avoid 403 errors"""
        chrome_options = Options()
        
        # Add user agent to avoid bot detection
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Additional options to appear more like a regular browser
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--lang=en-GB')
        
        # Uncomment the next line if you want to run headless (without browser window)
        # chrome_options.add_argument('--headless')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        
        # Execute script to remove webdriver property
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def handle_cookie_consent(self):
        """Try to handle cookie consent popup if it appears"""
        try:
            # Wait a bit for popup to appear
            time.sleep(2)
            
            # Try to find and switch to the cookie consent iframe
            try:
                iframe = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[id*='sp_message_iframe']"))
                )
                self.driver.switch_to.frame(iframe)
                
                # Try to click accept button
                accept_button = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'AGREE') or contains(text(), 'Accept') or contains(text(), 'Agree')]"))
                )
                accept_button.click()
                
                # Switch back to main content
                self.driver.switch_to.default_content()
                print(f"    ✓ Cookie consent accepted")
                time.sleep(1)
                return True
            except:
                # If no iframe found, try direct button click
                try:
                    self.driver.switch_to.default_content()
                    accept_button = self.driver.find_element(By.XPATH, "//button[contains(text(), 'AGREE') or contains(text(), 'Accept')]")
                    accept_button.click()
                    print(f"    ✓ Cookie consent accepted (direct)")
                    time.sleep(1)
                    return True
                except:
                    # No cookie popup found, that's ok
                    self.driver.switch_to.default_content()
                    return False
        except Exception as e:
            self.driver.switch_to.default_content()
            return False
    
    def search_and_navigate_to_player(self, player_name):
        """Search for player and navigate to their profile page"""
        try:
            # Build search URL
            search_url = f"https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche?query={player_name.replace(' ', '+')}"
            print(f"    → Searching: {search_url}")
            self.driver.get(search_url)
            
            # Handle cookie consent on first search
            self.handle_cookie_consent()
            
            # Wait for search results page to load
            time.sleep(2)
            
            try:
                # Try multiple selectors to find the player profile link
                player_link = None
                
                # Selector 1: Direct link with href containing "/profil/spieler/"
                try:
                    player_link = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/profil/spieler/']"))
                    )
                except:
                    pass
                
                # Selector 2: Link in table with title attribute
                if not player_link:
                    try:
                        player_link = self.driver.find_element(By.XPATH, "//a[@title and contains(@href, '/profil/spieler/')]")
                    except:
                        pass
                
                if not player_link:
                    print(f"    ✗ No results found for: {player_name}")
                    return False
                
                # Get the profile URL - DON'T CLICK, navigate directly instead
                profile_url = player_link.get_attribute('href')
                print(f"    → Found profile: {profile_url}")
                
                # Navigate directly to profile URL (avoids cookie popup click issues)
                self.driver.get(profile_url)
                
                # Wait for the profile page to load
                time.sleep(3)
                
                return True
                
            except TimeoutException:
                print(f"    ✗ No results found for: {player_name}")
                return False
                
        except Exception as e:
            print(f"    ✗ Error navigating to player: {str(e)}")
            return False
    
    def scrape_player_info(self):
        """Scrape player information from current profile page"""
        try:
            player_info = {}
            
            # Wait for the data-header__info-box to be present
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.data-header__info-box"))
                )
            except TimeoutException:
                print(f"    ✗ Profile page did not load properly")
                return None
            
            # Extract date of birth and age from data-header__info-box
            try:
                dob_element = self.driver.find_element(By.XPATH, 
                    "//span[contains(text(), 'Date of birth')]/following-sibling::span[1]")
                dob_text = dob_element.text.strip()
                player_info['date_of_birth'] = dob_text
                
                # Extract age separately if present
                age_match = re.search(r'\((\d+)\)', dob_text)
                if age_match:
                    player_info['age'] = int(age_match.group(1))
                else:
                    player_info['age'] = None
            except:
                try:
                    # Alternative: Look in info-table
                    dob_element = self.driver.find_element(By.XPATH, 
                        "//span[text()='Date of birth/Age:']/following-sibling::span[1]")
                    dob_text = dob_element.text.strip()
                    player_info['date_of_birth'] = dob_text
                    age_match = re.search(r'\((\d+)\)', dob_text)
                    if age_match:
                        player_info['age'] = int(age_match.group(1))
                except:
                    player_info['date_of_birth'] = None
                    player_info['age'] = None
            
            # Extract place of birth
            try:
                pob_element = self.driver.find_element(By.XPATH, 
                    "//span[contains(text(), 'Place of birth')]/following-sibling::span[1]")
                player_info['place_of_birth'] = pob_element.text.strip()
            except:
                try:
                    pob_element = self.driver.find_element(By.XPATH, 
                        "//span[text()='Place of birth:']/following-sibling::span[1]")
                    player_info['place_of_birth'] = pob_element.text.strip()
                except:
                    player_info['place_of_birth'] = None
            
            # Extract citizenship
            try:
                citizenship_element = self.driver.find_element(By.XPATH, 
                    "//span[contains(text(), 'Citizenship')]/following-sibling::span[1]")
                player_info['citizenship'] = citizenship_element.text.strip()
            except:
                try:
                    citizenship_element = self.driver.find_element(By.XPATH, 
                        "//span[text()='Citizenship:']/following-sibling::span[1]")
                    player_info['citizenship'] = citizenship_element.text.strip()
                except:
                    player_info['citizenship'] = None
            
            # Extract height
            try:
                height_element = self.driver.find_element(By.XPATH, 
                    "//span[contains(text(), 'Height')]/following-sibling::span[1]")
                player_info['height'] = height_element.text.strip()
            except:
                try:
                    height_element = self.driver.find_element(By.XPATH, 
                        "//span[text()='Height:']/following-sibling::span[1]")
                    player_info['height'] = height_element.text.strip()
                except:
                    player_info['height'] = None
            
            # Extract position
            try:
                position_element = self.driver.find_element(By.XPATH, 
                    "//span[contains(text(), 'Position')]/following-sibling::span[1]")
                player_info['position'] = position_element.text.strip()
            except:
                try:
                    position_element = self.driver.find_element(By.XPATH, 
                        "//span[text()='Position:']/following-sibling::span[1]")
                    player_info['position'] = position_element.text.strip()
                except:
                    player_info['position'] = None
            
            # Extract current international
            try:
                international_element = self.driver.find_element(By.XPATH, 
                    "//span[contains(text(), 'Current international')]/following-sibling::span[1]")
                player_info['current_international'] = international_element.text.strip()
            except:
                try:
                    international_element = self.driver.find_element(By.XPATH, 
                        "//span[text()='Current international:']/following-sibling::span[1]")
                    player_info['current_international'] = international_element.text.strip()
                except:
                    player_info['current_international'] = None
            
            # Extract caps/goals for national team
            try:
                caps_element = self.driver.find_element(By.XPATH, 
                    "//span[contains(text(), 'Caps/Goals')]/following-sibling::a[1]")
                caps_text = caps_element.text.strip()
                player_info['caps_goals'] = caps_text
                
                # Parse caps and goals separately
                caps_match = re.search(r'(\d+)\s*/\s*(\d+)', caps_text)
                if caps_match:
                    player_info['caps'] = int(caps_match.group(1))
                    player_info['goals'] = int(caps_match.group(2))
                else:
                    player_info['caps'] = None
                    player_info['goals'] = None
            except:
                try:
                    caps_element = self.driver.find_element(By.XPATH, 
                        "//span[text()='Caps/Goals:']/following-sibling::a[1]")
                    caps_text = caps_element.text.strip()
                    player_info['caps_goals'] = caps_text
                    
                    caps_match = re.search(r'(\d+)\s*/\s*(\d+)', caps_text)
                    if caps_match:
                        player_info['caps'] = int(caps_match.group(1))
                        player_info['goals'] = int(caps_match.group(2))
                except:
                    player_info['caps_goals'] = None
                    player_info['caps'] = None
                    player_info['goals'] = None
            
            # Extract market value from data-header__market-value-wrapper
            try:
                # First try: The link with class data-header__market-value-wrapper
                market_value_element = self.driver.find_element(By.CSS_SELECTOR, 
                    "a.data-header__market-value-wrapper")
                market_value_text = market_value_element.text.strip()
                # Clean up the text (remove "Market value:" if present)
                if "Market value:" in market_value_text:
                    market_value_text = market_value_text.replace("Market value:", "").strip()
                player_info['market_value'] = market_value_text
            except:
                try:
                    # Second try: Look for any element with market value info
                    market_value_element = self.driver.find_element(By.XPATH, 
                        "//a[contains(@href, '/marktwertverlauf/spieler/')]")
                    player_info['market_value'] = market_value_element.text.strip()
                except:
                    player_info['market_value'] = None
            
            player_info['profile_url'] = self.driver.current_url
            player_info['scraped_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return player_info
            
        except Exception as e:
            print(f"    ✗ Error scraping player info: {str(e)}")
            return None
    
    def get_player_info(self, player_name):
        """Get player info from cache or scrape it"""
        # Check cache first
        if player_name in self.player_cache:
            print(f"    ✓ Using cached info for {player_name}")
            return self.player_cache[player_name]
        
        # Search and navigate to player profile
        if not self.search_and_navigate_to_player(player_name):
            return None
        
        # Scrape player information from the profile page
        player_info = self.scrape_player_info()
        if player_info:
            # Cache the result
            self.player_cache[player_name] = player_info
            print(f"    ✓ Successfully scraped info for {player_name}")
        else:
            print(f"    ✗ Failed to scrape info for {player_name}")
        
        # Add delay to avoid rate limiting
        time.sleep(3)
        
        return player_info
    
    def process_json_file(self, json_path, season_folder):
        """Process a single JSON file and add player info"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if data is a list
            if not isinstance(data, list):
                print(f"  Warning: Expected list but got {type(data)} in {json_path}")
                return None
            
            print(f"\n{'='*60}")
            print(f"Processing {season_folder}: Found {len(data)} players")
            print(f"{'='*60}")
            
            updated_count = 0
            skipped_count = 0
            error_count = 0
            
            # Process each player in the list
            for i, player_data in enumerate(data):
                player_name = player_data.get('Player', '')
                if not player_name:
                    print(f"  [Player {i+1}/{len(data)}] No player name found, skipping")
                    skipped_count += 1
                    continue
                
                # Check if player info already exists
                if 'player_info' in player_data and player_data['player_info']:
                    print(f"  [Player {i+1}/{len(data)}] {player_name} - Info already exists, skipping")
                    skipped_count += 1
                    continue
                
                print(f"\n  [Player {i+1}/{len(data)}] Processing: {player_name}")
                
                # Get player information
                player_info = self.get_player_info(player_name)
                if player_info:
                    player_data['player_info'] = player_info
                    updated_count += 1
                else:
                    error_count += 1
            
            print(f"\n{'-'*60}")
            print(f"Season {season_folder} Summary:")
            print(f"  ✓ Updated: {updated_count}")
            print(f"  → Skipped: {skipped_count}")
            print(f"  ✗ Errors: {error_count}")
            print(f"{'-'*60}")
            
            return data
            
        except json.JSONDecodeError as e:
            print(f"  Error: Invalid JSON in {json_path}: {str(e)}")
            return None
        except Exception as e:
            print(f"  Error processing {json_path}: {str(e)}")
            return None
    
    def process_all_files(self, base_folder):
        """Process all JSON files in all season folders"""
        processed_count = 0
        error_count = 0
        total_players_updated = 0
        
        # Get all season folders
        season_folders = [f for f in os.listdir(base_folder) 
                         if os.path.isdir(os.path.join(base_folder, f))]
        season_folders.sort()
        
        print(f"\n{'='*60}")
        print(f"TRANSFERMARKT SCRAPER STARTED")
        print(f"{'='*60}")
        print(f"Found {len(season_folders)} season folders to process")
        print(f"Output folder: {self.output_folder}")
        print(f"{'='*60}\n")
        
        for season_folder in season_folders:
            season_path = os.path.join(base_folder, season_folder)
            json_file = os.path.join(season_path, 'player_stats.json')
            
            if not os.path.exists(json_file):
                print(f"\n{season_folder}: No player_stats.json found, skipping")
                continue
            
            # Process the JSON file
            updated_data = self.process_json_file(json_file, season_folder)
            
            if updated_data:
                # Count how many players were updated
                players_with_info = sum(1 for p in updated_data if 'player_info' in p and p['player_info'])
                total_players_updated += players_with_info
                
                # Save to output folder
                output_file = os.path.join(self.output_folder, f"{season_folder}_player_info.json")
                try:
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(updated_data, f, indent=4, ensure_ascii=False)
                    print(f"\n  ✓ Saved to: {output_file}")
                    processed_count += 1
                except Exception as e:
                    print(f"\n  ✗ Error saving file: {str(e)}")
                    error_count += 1
            else:
                error_count += 1
        
        print(f"\n{'='*60}")
        print(f"SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"Season folders processed: {processed_count}/{len(season_folders)}")
        print(f"Total players with info: {total_players_updated}")
        print(f"Unique players scraped: {len(self.player_cache)}")
        print(f"Errors: {error_count}")
        print(f"{'='*60}\n")
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()

def main():
    # Configuration
    BASE_FOLDER = r"C:\Users\dell\OneDrive\Desktop\Sports Analytics & Team Performance\data\processed\DW JasonFiles(2)"
    OUTPUT_FOLDER = r"C:\Users\dell\OneDrive\Desktop\players_info"
    
    # Create scraper instance
    scraper = TransfermarktScraper(OUTPUT_FOLDER)
    
    try:
        # Process all files
        scraper.process_all_files(BASE_FOLDER)
    except KeyboardInterrupt:
        print("\n\n⚠ Script interrupted by user. Closing browser...")
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {str(e)}")
    finally:
        # Always close the browser
        scraper.close()
        print("Browser closed.")

if __name__ == "__main__":
    main()