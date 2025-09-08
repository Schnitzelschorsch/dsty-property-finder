# crawler.py - Core DSTY Property Crawler for Railway
import requests
import sqlite3
import time
import json
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DStyPropertyCrawler:
    def __init__(self):
        self.db_path = "dsty_properties.db"
        self.setup_database()
        
        # DSTY-specific search areas with priorities
        self.target_areas = {
            # Pink Route (Premium) - Highest priority
            '田園調布': {'priority': 10, 'route': 'Pink', 'suumo_code': '13111'},
            '目黒': {'priority': 10, 'route': 'Pink', 'suumo_code': '13109'},
            '恵比寿': {'priority': 9, 'route': 'Pink', 'suumo_code': '13109'},
            
            # Yellow Route (Excellent) - High priority  
            '等々力': {'priority': 8, 'route': 'Yellow', 'suumo_code': '13112'},
            '尾山台': {'priority': 8, 'route': 'Yellow', 'suumo_code': '13112'},
            '都立大学': {'priority': 8, 'route': 'Yellow', 'suumo_code': '13114'},
            
            # Green Route (Good) - Medium priority
            '三軒茶屋': {'priority': 7, 'route': 'Green', 'suumo_code': '13112'},
            '駒沢大学': {'priority': 7, 'route': 'Green', 'suumo_code': '13112'},
            
            # Near school - Budget priority
            '仲町台': {'priority': 6, 'route': 'School', 'suumo_code': '14108'},
        }
        
        # Your criteria
        self.min_rent = 250000
        self.max_rent = 350000
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })

    def setup_database(self):
        """Setup SQLite database for storing properties"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            price INTEGER,
            rooms TEXT,
            location TEXT,
            station TEXT,
            walk_minutes INTEGER,
            property_url TEXT UNIQUE,
            found_date TEXT,
            score REAL,
            area_priority INTEGER,
            route_type TEXT,
            reasons TEXT,
            is_active BOOLEAN DEFAULT 1
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_date TEXT,
            area TEXT,
            properties_found INTEGER,
            new_properties INTEGER
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized")

    def search_suumo_area(self, area_name, area_data):
        """Search Suumo for specific area"""
        properties = []
        
        try:
            # Build Suumo search URL with your exact criteria
            params = {
                'ar': '030',  # Kanto region
                'bs': '040',  # Building type
                'ta': '13',   # Tokyo (or 14 for Kanagawa)
                'sc': area_data['suumo_code'],  # Area code
                'cb': str(self.min_rent / 10000),  # Min rent in 万円
                'ct': str(self.max_rent / 10000),  # Max rent in 万円
                'mb': '0',    # Min walk time
                'mt': '15',   # Max 15 min walk
                'shkr1': '03', # 3+ rooms
                'shkr2': '03',
                'shkr3': '03',
                'rn': '0005'  # Sort by newest
            }
            
            url = f"https://suumo.jp/jj/chintai/ichiran/FR301FC001/?{urlencode(params)}"
            logger.info(f"Searching {area_name}: {url[:100]}...")
            
            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to fetch {area_name}: {response.status_code}")
                return properties
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find property listings
            property_items = soup.find_all('div', class_='cassetteitem')
            logger.info(f"Found {len(property_items)} properties in {area_name}")
            
            for item in property_items[:10]:  # Limit to first 10
                property_data = self.parse_property_item(item, area_name, area_data)
                if property_data:
                    properties.append(property_data)
            
            # Be respectful - delay between requests
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"Error searching {area_name}: {e}")
        
        return properties

    def parse_property_item(self, item, area_name, area_data):
        """Parse individual property from Suumo HTML"""
        try:
            # Extract title
            title_elem = item.find('div', class_='cassetteitem_content-title')
            title = title_elem.get_text(strip=True) if title_elem else "No title"
            
            # Extract price
            price_elem = item.find('span', class_='cassetteitem_price--rent')
            if not price_elem:
                return None
            
            price_text = price_elem.get_text(strip=True)
            price = self.extract_price(price_text)
            
            if not (self.min_rent <= price <= self.max_rent):
                return None  # Skip if outside budget
            
            # Extract room layout
            room_elem = item.find('span', class_='cassetteitem_madori')
            rooms = room_elem.get_text(strip=True) if room_elem else "Unknown"
            
            # Extract location details
            detail_col = item.find('li', class_='cassetteitem_detail-col1')
            location = detail_col.get_text(strip=True) if detail_col else area_name
            
            # Extract walk time to station
            walk_elem = item.find('span', class_='cassetteitem_detail-text')
            walk_minutes = self.extract_walk_time(walk_elem.get_text() if walk_elem else "")
            
            # Extract property URL
            link_elem = item.find('a')
            if link_elem and link_elem.get('href'):
                property_url = f"https://suumo.jp{link_elem['href']}"
            else:
                property_url = ""
            
            # Create property data
            property_data = {
                'title': title,
                'price': price,
                'rooms': rooms,
                'location': location,
                'station': area_name,
                'walk_minutes': walk_minutes,
                'property_url': property_url,
                'found_date': datetime.now().isoformat(),
                'area_priority': area_data['priority'],
                'route_type': area_data['route']
            }
            
            # Calculate score and reasons
            score, reasons = self.calculate_score(property_data, area_data)
            property_data['score'] = score
            property_data['reasons'] = json.dumps(reasons)
            
            return property_data
            
        except Exception as e:
            logger.error(f"Error parsing property: {e}")
            return None

    def extract_price(self, price_text):
        """Extract price from Japanese text like '28万円'"""
        # Remove Japanese characters and extract number
        numbers = re.findall(r'(\d+\.?\d*)', price_text.replace('万', '').replace('円', ''))
        if numbers:
            return int(float(numbers[0]) * 10000)  # Convert 万円 to yen
        return 0

    def extract_walk_time(self, text):
        """Extract walk time like '徒歩5分' -> 5"""
        numbers = re.findall(r'(\d+)分', text)
        return int(numbers[0]) if numbers else 99

    def calculate_score(self, property_data, area_data):
        """Calculate property score based on DSTY criteria"""
        score = 0
        reasons = []
        
        # Price scoring (25 points max)
        price = property_data['price']
        if 280000 <= price <= 320000:
            score += 25
            reasons.append("Perfect price range")
        elif 250000 <= price <= 350000:
            score += 20
            reasons.append("Good price")
        
        # Room scoring (20 points max)
        rooms = property_data['rooms']
        if rooms == "3LDK":
            score += 20
            reasons.append("Perfect 3LDK layout")
        elif rooms == "2LDK":
            score += 15
            reasons.append("Good 2LDK layout")
        elif "3" in rooms:
            score += 18
            reasons.append("3-room layout")
        
        # Area priority (20 points max)
        score += area_data['priority']
        reasons.append(f"Priority area: {area_data['route']} Route")
        
        # Walk time scoring (15 points max)
        walk = property_data['walk_minutes']
        if walk <= 5:
            score += 15
            reasons.append("Very close to station")
        elif walk <= 10:
            score += 10
            reasons.append("Close to station")
        elif walk <= 15:
            score += 5
            reasons.append("Acceptable walk")
        
        # DSTY bus route bonus (10 points max)
        route = area_data['route']
        if route == 'Pink':
            score += 10
            reasons.append("Premium Pink Route access")
        elif route == 'Yellow':
            score += 8
            reasons.append("Excellent Yellow Route access")
        elif route == 'Green':
            score += 6
            reasons.append("Good Green Route access")
        
        return score, reasons

    def save_properties(self, properties):
        """Save properties to database"""
        if not properties:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_count = 0
        for prop in properties:
            try:
                cursor.execute('''
                INSERT OR IGNORE INTO properties 
                (title, price, rooms, location, station, walk_minutes, property_url,
                 found_date, score, area_priority, route_type, reasons)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    prop['title'], prop['price'], prop['rooms'], prop['location'],
                    prop['station'], prop['walk_minutes'], prop['property_url'],
                    prop['found_date'], prop['score'], prop['area_priority'],
                    prop['route_type'], prop['reasons']
                ))
                
                if cursor.rowcount > 0:
                    new_count += 1
                    
            except sqlite3.Error as e:
                logger.error(f"Database error: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Saved {new_count} new properties out of {len(properties)} found")
        return new_count

    def log_search(self, area, found, new):
        """Log search activity"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO search_log (search_date, area, properties_found, new_properties)
        VALUES (?, ?, ?, ?)
        ''', (datetime.now().isoformat(), area, found, new))
        
        conn.commit()
        conn.close()

    def run_full_search(self):
        """Run comprehensive search across all target areas"""
        logger.info("🏠 Starting DSTY property search...")
        
        total_found = 0
        total_new = 0
        
        # Search each target area
        for area_name, area_data in self.target_areas.items():
            logger.info(f"🔍 Searching {area_name} ({area_data['route']} Route)...")
            
            properties = self.search_suumo_area(area_name, area_data)
            
            if properties:
                new_count = self.save_properties(properties)
                self.log_search(area_name, len(properties), new_count)
                
                total_found += len(properties)
                total_new += new_count
                
                logger.info(f"✅ {area_name}: {len(properties)} found, {new_count} new")
            else:
                logger.info(f"❌ {area_name}: No properties found")
        
        logger.info(f"🎉 Search complete! Total: {total_found} found, {total_new} new")
        return total_found, total_new

    def get_top_properties(self, limit=20):
        """Get top-ranked properties"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT * FROM properties 
        WHERE is_active = 1 
        ORDER BY score DESC 
        LIMIT ?
        ''', (limit,))
        
        columns = [desc[0] for desc in cursor.description]
        properties = []
        
        for row in cursor.fetchall():
            prop_dict = dict(zip(columns, row))
            prop_dict['reasons'] = json.loads(prop_dict.get('reasons', '[]'))
            properties.append(prop_dict)
        
        conn.close()
        return properties

    def get_stats(self):
        """Get search statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1')
        total = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1 AND price BETWEEN ? AND ?', 
                      (self.min_rent, self.max_rent))
        in_budget = cursor.fetchone()[0]
        
        cursor.execute('SELECT AVG(score) FROM properties WHERE is_active = 1')
        avg_score = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT MAX(score) FROM properties WHERE is_active = 1')
        max_score = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            'total_properties': total,
            'in_budget': in_budget,
            'avg_score': round(avg_score, 1),
            'max_score': round(max_score, 1)
        }

# Test run
if __name__ == "__main__":
    crawler = DStyPropertyCrawler()
    crawler.run_full_search()
