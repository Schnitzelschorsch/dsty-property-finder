# simple_crawler.py - Working Suumo-only DSTY Crawler
import requests
import sqlite3
import time
import json
import logging
import random
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleDStyPropertyCrawler:
    def __init__(self):
        self.db_path = "dsty_properties.db"
        self.setup_database()
        
        # Simplified DSTY target stations (just the main ones)
        self.target_stations = {
            '田園調布': {'priority': 10, 'route': 'Pink'},
            '目黒': {'priority': 10, 'route': 'Pink'},
            '恵比寿': {'priority': 9, 'route': 'Pink'},
            '等々力': {'priority': 8, 'route': 'Yellow'},
            '尾山台': {'priority': 8, 'route': 'Yellow'},
            '三軒茶屋': {'priority': 7, 'route': 'Green'},
        }
        
        # Broader search criteria for better results
        self.min_rent = 200000  # Broader range
        self.max_rent = 400000  # Broader range
        
        # Simple session setup
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })

    def setup_database(self):
        """Simple database setup"""
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
            source TEXT,
            score REAL,
            area_priority INTEGER,
            route_type TEXT,
            reasons TEXT,
            is_active BOOLEAN DEFAULT 1
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Simple database initialized")

    def search_suumo_station(self, station_name, station_data):
        """Search Suumo by station name with broader parameters"""
        properties = []
        
        try:
            # Use station-based search (simpler and more reliable)
            station_encoded = quote(station_name)
            
            # Broader search parameters
            params = {
                'ar': '030',  # Kanto region
                'bs': '040',  # Building type
                'ra': '013',  # Tokyo area
                'rn': '0005', # Sort by newest
                'ek': station_encoded,  # Station name
                'cb': '20.0',  # Min rent (20万円)
                'ct': '40.0',  # Max rent (40万円)
                'mt': '20',    # Max walk 20 minutes
                'md': '02',    # Include 2+ rooms
                'md': '03',    # Include 3+ rooms
            }
            
            url = f"https://suumo.jp/jj/chintai/ichiran/FR301FC005/?{urlencode(params)}"
            logger.info(f"Searching Suumo for properties near {station_name} station...")
            
            # Add random delay to be respectful
            time.sleep(random.uniform(3, 6))
            
            response = self.session.get(url, timeout=30)
            logger.info(f"Suumo response status for {station_name}: {response.status_code}")
            
            if response.status_code == 200:
                properties = self.parse_suumo_simple(response.text, station_name, station_data)
                logger.info(f"✅ Found {len(properties)} properties near {station_name}")
            else:
                logger.warning(f"⚠️ Suumo returned status {response.status_code} for {station_name}")
                
        except Exception as e:
            logger.error(f"❌ Error searching {station_name}: {e}")
        
        return properties

    def parse_suumo_simple(self, html_content, station_name, station_data):
        """Simple HTML parsing focused on getting any properties"""
        properties = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for any property containers (try multiple selectors)
            property_containers = (
                soup.find_all('div', class_='cassetteitem') or
                soup.find_all('div', {'data-bc': True}) or
                soup.find_all('div', class_='property') or
                soup.find_all('article') or
                soup.find_all('div', class_='item')
            )
            
            logger.info(f"Found {len(property_containers)} potential property containers for {station_name}")
            
            for i, container in enumerate(property_containers[:10]):  # Process first 10
                try:
                    property_data = self.extract_property_data_simple(container, station_name, station_data)
                    if property_data:
                        properties.append(property_data)
                        logger.info(f"Successfully parsed property {i+1}: {property_data['title'][:50]}...")
                except Exception as e:
                    logger.debug(f"Could not parse property container {i+1}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error parsing HTML for {station_name}: {e}")
        
        return properties

    def extract_property_data_simple(self, container, station_name, station_data):
        """Extract property data with simple fallbacks"""
        try:
            # Extract title (try multiple approaches)
            title = self.find_text_by_selectors(container, [
                'div.cassetteitem_content-title',
                'h2', 'h3', 'h4',
                '[class*="title"]',
                '[class*="name"]'
            ]) or f"Property near {station_name}"
            
            # Extract price
            price_text = self.find_text_by_selectors(container, [
                'span.cassetteitem_price--rent',
                '[class*="rent"]',
                '[class*="price"]',
                'span[class*="yen"]'
            ])
            
            price = self.extract_price_simple(price_text) if price_text else 0
            
            # Skip if no valid price found
            if price < 50000:  # Less than 5万円 is probably not valid
                return None
            
            # Extract rooms
            rooms = self.find_text_by_selectors(container, [
                'span.cassetteitem_madori',
                '[class*="madori"]',
                '[class*="layout"]',
                '[class*="room"]'
            ]) or "2-3LDK"
            
            # Extract location
            location = self.find_text_by_selectors(container, [
                'li.cassetteitem_detail-col1',
                '[class*="address"]',
                '[class*="location"]'
            ]) or station_name
            
            # Extract walk time
            walk_text = container.get_text()
            walk_minutes = self.extract_walk_time_simple(walk_text)
            
            # Extract URL
            property_url = self.extract_url_simple(container)
            
            # Create property data
            property_data = {
                'title': title.strip(),
                'price': price,
                'rooms': rooms.strip(),
                'location': location.strip(),
                'station': station_name,
                'walk_minutes': walk_minutes,
                'property_url': property_url,
                'found_date': datetime.now().isoformat(),
                'source': 'Suumo',
                'area_priority': station_data['priority'],
                'route_type': station_data['route']
            }
            
            # Calculate score
            score, reasons = self.calculate_score_simple(property_data, station_data)
            property_data['score'] = score
            property_data['reasons'] = json.dumps(reasons)
            
            return property_data
            
        except Exception as e:
            logger.debug(f"Error extracting property data: {e}")
            return None

    def find_text_by_selectors(self, container, selectors):
        """Try multiple CSS selectors to find text"""
        for selector in selectors:
            try:
                element = container.select_one(selector)
                if element and element.get_text(strip=True):
                    return element.get_text(strip=True)
            except:
                continue
        return None

    def extract_price_simple(self, price_text):
        """Simple price extraction"""
        if not price_text:
            return 0
        
        # Clean up text
        clean_text = re.sub(r'[万円,\s]', '', price_text)
        
        # Look for number patterns
        patterns = [
            r'(\d+\.?\d*)万',  # X万円 format
            r'(\d{6,})',       # Direct yen amount 
            r'(\d+\.?\d*)',    # Any number
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, clean_text)
            if matches:
                try:
                    amount = float(matches[0])
                    if amount < 100:  # Assume it's in 万円
                        return int(amount * 10000)
                    else:
                        return int(amount)
                except:
                    continue
        
        return 0

    def extract_walk_time_simple(self, text):
        """Extract walk time from text"""
        walk_patterns = [r'徒歩(\d+)分', r'(\d+)分']
        
        for pattern in walk_patterns:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1))
        
        return 10  # Default assumption

    def extract_url_simple(self, container):
        """Extract property URL"""
        link = container.find('a')
        if link and link.get('href'):
            href = link['href']
            if href.startswith('/'):
                return f"https://suumo.jp{href}"
            elif href.startswith('http'):
                return href
        return "https://suumo.jp/"

    def calculate_score_simple(self, property_data, station_data):
        """Simple scoring algorithm"""
        score = 0
        reasons = []
        
        # Price scoring (flexible)
        price = property_data['price']
        if 250000 <= price <= 350000:
            score += 30
            reasons.append("Perfect DSTY budget range")
        elif 200000 <= price <= 400000:
            score += 25
            reasons.append("Good price range")
        elif price < 200000:
            score += 20
            reasons.append("Great value - very affordable")
        else:
            score += 10
            reasons.append("Higher price range")
        
        # Room scoring
        rooms = property_data['rooms']
        if '3LDK' in rooms or '3LD' in rooms:
            score += 25
            reasons.append("Perfect family size (3LDK)")
        elif '2LDK' in rooms or '2LD' in rooms:
            score += 20
            reasons.append("Good family size (2LDK)")
        elif '3' in rooms:
            score += 22
            reasons.append("3-room layout")
        elif '2' in rooms:
            score += 18
            reasons.append("2-room layout")
        
        # Area/Route scoring
        priority = station_data['priority']
        score += priority
        route = station_data['route']
        
        if route == 'Pink':
            reasons.append("Premium Pink Route - excellent DSTY bus access")
        elif route == 'Yellow':
            reasons.append("Excellent Yellow Route - great for families")
        elif route == 'Green':
            reasons.append("Good Green Route - nice residential area")
        
        # Walk time scoring
        walk = property_data['walk_minutes']
        if walk <= 10:
            score += 15
            reasons.append(f"Close to station ({walk} min walk)")
        elif walk <= 15:
            score += 10
            reasons.append(f"Reasonable walk to station ({walk} min)")
        elif walk <= 20:
            score += 5
            reasons.append(f"Acceptable walk to station ({walk} min)")
        
        # Bonus for good areas
        location = property_data['location'].lower()
        if any(keyword in location for keyword in ['田園調布', '目黒', '恵比寿']):
            score += 5
            reasons.append("Premium residential area")
        
        return min(100, max(0, score)), reasons

    def save_properties_simple(self, properties):
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
                 found_date, source, score, area_priority, route_type, reasons)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    prop['title'], prop['price'], prop['rooms'], prop['location'],
                    prop['station'], prop['walk_minutes'], prop['property_url'],
                    prop['found_date'], prop['source'], prop['score'],
                    prop['area_priority'], prop['route_type'], prop['reasons']
                ))
                
                if cursor.rowcount > 0:
                    new_count += 1
                    
            except sqlite3.Error as e:
                logger.error(f"Database error: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Saved {new_count} new properties out of {len(properties)} found")
        return new_count

    def run_simple_search(self):
        """Run simple Suumo-only search"""
        logger.info("🏠 Starting simple DSTY property search (Suumo only)...")
        
        total_found = 0
        total_new = 0
        
        for station_name, station_data in self.target_stations.items():
            logger.info(f"🔍 Searching near {station_name} station ({station_data['route']} Route)...")
            
            properties = self.search_suumo_station(station_name, station_data)
            
            if properties:
                new_count = self.save_properties_simple(properties)
                total_found += len(properties)
                total_new += new_count
                logger.info(f"✅ {station_name}: Found {len(properties)}, saved {new_count} new")
            else:
                logger.info(f"❌ {station_name}: No properties found")
        
        logger.info(f"🎉 Simple search complete! Total: {total_found} found, {total_new} new")
        return total_found, total_new

    # Compatibility methods
    def get_top_properties(self, limit=20):
        """Get top-ranked properties"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT * FROM properties 
        WHERE is_active = 1 
        ORDER BY score DESC, found_date DESC
        LIMIT ?
        ''', (limit,))
        
        columns = [desc[0] for desc in cursor.description]
        properties = []
        
        for row in cursor.fetchall():
            prop_dict = dict(zip(columns, row))
            try:
                prop_dict['reasons'] = json.loads(prop_dict.get('reasons', '[]'))
            except:
                prop_dict['reasons'] = []
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
                      (250000, 350000))
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

    # Alias for app.py compatibility
    def run_full_search(self):
        return self.run_simple_search()

if __name__ == "__main__":
    crawler = SimpleDStyPropertyCrawler()
    crawler.run_simple_search()
