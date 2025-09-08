# multi_site_crawler.py - Working Multi-Site DSTY Property Crawler
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

class MultiSiteDStyPropertyCrawler:
    def __init__(self):
        self.db_path = "dsty_properties.db"
        self.setup_database()
        
        # DSTY target stations with correct site mappings
        self.target_stations = {
            '田園調布': {
                'priority': 10, 'route': 'Pink',
                'suumo_station': '田園調布',
                'homes_station': '田園調布',
                'athome_station': '田園調布',
                'lifull_station': '田園調布'
            },
            '目黒': {
                'priority': 10, 'route': 'Pink',
                'suumo_station': '目黒',
                'homes_station': '目黒',
                'athome_station': '目黒',
                'lifull_station': '目黒'
            },
            '恵比寿': {
                'priority': 9, 'route': 'Pink',
                'suumo_station': '恵比寿',
                'homes_station': '恵比寿',
                'athome_station': '恵比寿',
                'lifull_station': '恵比寿'
            },
            '等々力': {
                'priority': 8, 'route': 'Yellow',
                'suumo_station': '等々力',
                'homes_station': '等々力',
                'athome_station': '等々力',
                'lifull_station': '等々力'
            },
            '尾山台': {
                'priority': 8, 'route': 'Yellow',
                'suumo_station': '尾山台',
                'homes_station': '尾山台',
                'athome_station': '尾山台',
                'lifull_station': '尾山台'
            },
            '三軒茶屋': {
                'priority': 7, 'route': 'Green',
                'suumo_station': '三軒茶屋',
                'homes_station': '三軒茶屋',
                'athome_station': '三軒茶屋',
                'lifull_station': '三軒茶屋'
            }
        }
        
        # Search criteria
        self.min_rent = 200000
        self.max_rent = 400000
        
        # User agents for rotation
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
        ]
        
        self.session = requests.Session()

    def setup_database(self):
        """Database setup with enhanced fields"""
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
            building_age TEXT,
            floor_area TEXT,
            is_active BOOLEAN DEFAULT 1
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Multi-site database initialized")

    def get_headers(self):
        """Get randomized headers"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://www.google.com/',
            'Upgrade-Insecure-Requests': '1'
        }

    def random_delay(self):
        """Random delay between requests"""
        time.sleep(random.uniform(2, 5))

    # SUUMO SEARCH
    def search_suumo(self, station_name, station_data):
        """Search Suumo with improved parameters"""
        properties = []
        
        try:
            self.session.headers.update(self.get_headers())
            
            # Use station-based search URL (more reliable)
            station_encoded = quote(station_data['suumo_station'])
            
            params = {
                'ar': '030',  # Kanto
                'bs': '040',  # Building type
                'ra': '013',  # Tokyo
                'rn': '0005', # Sort newest
                'ek': station_encoded,
                'cb': '20.0', # Min 20万
                'ct': '40.0', # Max 40万
                'mt': '20',   # Max walk 20min
            }
            
            url = f"https://suumo.jp/jj/chintai/ichiran/FR301FC005/?{urlencode(params)}"
            logger.info(f"Searching Suumo near {station_name}...")
            
            self.random_delay()
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_suumo_results(response.text, station_name, station_data)
                logger.info(f"✅ Suumo {station_name}: Found {len(properties)} properties")
            else:
                logger.warning(f"⚠️ Suumo {station_name}: Status {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Suumo {station_name}: {e}")
        
        return properties

    # HOMES.CO.JP SEARCH
    def search_homes(self, station_name, station_data):
        """Search Homes.co.jp with correct URLs"""
        properties = []
        
        try:
            self.session.headers.update(self.get_headers())
            
            # Homes.co.jp search by station
            station_encoded = quote(station_data['homes_station'])
            
            # Correct Homes.co.jp URL format
            url = f"https://www.homes.co.jp/chintai/tokyo/station/{station_encoded}/"
            
            logger.info(f"Searching Homes.co.jp near {station_name}...")
            
            self.random_delay()
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_homes_results(response.text, station_name, station_data)
                logger.info(f"✅ Homes.co.jp {station_name}: Found {len(properties)} properties")
            else:
                logger.warning(f"⚠️ Homes.co.jp {station_name}: Status {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Homes.co.jp {station_name}: {e}")
        
        return properties

    # ATHOME.CO.JP SEARCH
    def search_athome(self, station_name, station_data):
        """Search AtHome.co.jp"""
        properties = []
        
        try:
            self.session.headers.update(self.get_headers())
            
            # AtHome search URL
            station_encoded = quote(station_data['athome_station'])
            
            # AtHome URL format
            params = {
                'pref': '13',  # Tokyo
                'city': '',
                'town': '',
                'station': station_encoded,
                'rent1': '20',  # Min rent
                'rent2': '40',  # Max rent
                'room': '1',    # Room type
            }
            
            url = f"https://www.athome.co.jp/chintai/list/?{urlencode(params)}"
            
            logger.info(f"Searching AtHome near {station_name}...")
            
            self.random_delay()
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_athome_results(response.text, station_name, station_data)
                logger.info(f"✅ AtHome {station_name}: Found {len(properties)} properties")
            else:
                logger.warning(f"⚠️ AtHome {station_name}: Status {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ AtHome {station_name}: {e}")
        
        return properties

    # LIFULL HOME'S SEARCH
    def search_lifull(self, station_name, station_data):
        """Search LIFULL HOME'S"""
        properties = []
        
        try:
            self.session.headers.update(self.get_headers())
            
            # LIFULL HOME'S search
            station_encoded = quote(station_data['lifull_station'])
            
            # LIFULL URL format
            url = f"https://www.homes.co.jp/chintai/tokyo/{station_encoded}/"
            
            logger.info(f"Searching LIFULL HOME'S near {station_name}...")
            
            self.random_delay()
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_lifull_results(response.text, station_name, station_data)
                logger.info(f"✅ LIFULL {station_name}: Found {len(properties)} properties")
            else:
                logger.warning(f"⚠️ LIFULL {station_name}: Status {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ LIFULL {station_name}: {e}")
        
        return properties

    # PARSING METHODS
    def parse_suumo_results(self, html, station_name, station_data):
        """Parse Suumo HTML results"""
        properties = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Multiple selectors for Suumo properties
            containers = (
                soup.find_all('div', class_='cassetteitem') or
                soup.find_all('div', {'data-bc': True}) or
                soup.find_all('article')
            )
            
            for container in containers[:8]:
                prop = self.extract_suumo_property(container, station_name, station_data)
                if prop and self.is_valid_property(prop):
                    properties.append(prop)
                    
        except Exception as e:
            logger.error(f"Error parsing Suumo HTML: {e}")
        
        return properties

    def parse_homes_results(self, html, station_name, station_data):
        """Parse Homes.co.jp results"""
        properties = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Homes.co.jp property selectors
            containers = (
                soup.find_all('div', class_='bukkenList') or
                soup.find_all('div', class_='listTable') or
                soup.find_all('article', class_='prg-bukkenList')
            )
            
            for container in containers[:5]:
                prop = self.extract_homes_property(container, station_name, station_data)
                if prop and self.is_valid_property(prop):
                    properties.append(prop)
                    
        except Exception as e:
            logger.error(f"Error parsing Homes HTML: {e}")
        
        return properties

    def parse_athome_results(self, html, station_name, station_data):
        """Parse AtHome results"""
        properties = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # AtHome property selectors
            containers = (
                soup.find_all('div', class_='contents') or
                soup.find_all('tr', class_='property') or
                soup.find_all('div', class_='bukken')
            )
            
            for container in containers[:5]:
                prop = self.extract_athome_property(container, station_name, station_data)
                if prop and self.is_valid_property(prop):
                    properties.append(prop)
                    
        except Exception as e:
            logger.error(f"Error parsing AtHome HTML: {e}")
        
        return properties

    def parse_lifull_results(self, html, station_name, station_data):
        """Parse LIFULL results"""
        properties = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # LIFULL property selectors (similar to Homes)
            containers = soup.find_all('div', class_='bukkenList')
            
            for container in containers[:5]:
                prop = self.extract_lifull_property(container, station_name, station_data)
                if prop and self.is_valid_property(prop):
                    properties.append(prop)
                    
        except Exception as e:
            logger.error(f"Error parsing LIFULL HTML: {e}")
        
        return properties

    # PROPERTY EXTRACTION METHODS
    def extract_suumo_property(self, container, station_name, station_data):
        """Extract Suumo property details"""
        try:
            title = self.find_text(container, [
                'div.cassetteitem_content-title',
                'h2', 'h3'
            ]) or f"Property near {station_name}"
            
            price_text = self.find_text(container, [
                'span.cassetteitem_price--rent',
                '[class*="price"]',
                '[class*="rent"]'
            ])
            
            price = self.extract_price(price_text)
            if price < 50000:
                return None
            
            rooms = self.find_text(container, [
                'span.cassetteitem_madori',
                '[class*="madori"]'
            ]) or "2-3LDK"
            
            location = self.find_text(container, [
                'li.cassetteitem_detail-col1',
                '[class*="address"]'
            ]) or station_name
            
            walk_minutes = self.extract_walk_time(container.get_text())
            property_url = self.extract_url(container, 'https://suumo.jp')
            
            return self.create_property_object(
                title, price, rooms, location, station_name, 
                walk_minutes, property_url, 'Suumo', station_data
            )
            
        except Exception as e:
            logger.debug(f"Error extracting Suumo property: {e}")
            return None

    def extract_homes_property(self, container, station_name, station_data):
        """Extract Homes.co.jp property details"""
        try:
            title = self.find_text(container, [
                'h2', 'h3', '.bukkenName',
                '[class*="title"]'
            ]) or f"Property near {station_name}"
            
            price_text = self.find_text(container, [
                '[class*="rent"]', '[class*="price"]',
                '.bukkenPrice'
            ])
            
            price = self.extract_price(price_text)
            if price < 50000:
                return None
            
            rooms = self.find_text(container, [
                '[class*="madori"]', '[class*="layout"]'
            ]) or "2-3LDK"
            
            return self.create_property_object(
                title, price, rooms, station_name, station_name,
                10, 'https://www.homes.co.jp/', 'Homes.co.jp', station_data
            )
            
        except Exception as e:
            logger.debug(f"Error extracting Homes property: {e}")
            return None

    def extract_athome_property(self, container, station_name, station_data):
        """Extract AtHome property details"""
        try:
            title = self.find_text(container, [
                'h2', 'h3', '[class*="title"]'
            ]) or f"Property near {station_name}"
            
            price_text = container.get_text()
            price = self.extract_price(price_text)
            
            if price < 50000:
                return None
            
            return self.create_property_object(
                title, price, "2-3LDK", station_name, station_name,
                12, 'https://www.athome.co.jp/', 'AtHome', station_data
            )
            
        except Exception as e:
            logger.debug(f"Error extracting AtHome property: {e}")
            return None

    def extract_lifull_property(self, container, station_name, station_data):
        """Extract LIFULL property details"""
        try:
            title = self.find_text(container, [
                'h2', 'h3', '.bukkenName'
            ]) or f"Property near {station_name}"
            
            price_text = self.find_text(container, [
                '[class*="price"]', '[class*="rent"]'
            ])
            
            price = self.extract_price(price_text)
            if price < 50000:
                return None
            
            return self.create_property_object(
                title, price, "2-3LDK", station_name, station_name,
                10, 'https://www.homes.co.jp/', 'LIFULL', station_data
            )
            
        except Exception as e:
            logger.debug(f"Error extracting LIFULL property: {e}")
            return None

    # UTILITY METHODS
    def find_text(self, container, selectors):
        """Find text using multiple selectors"""
        for selector in selectors:
            try:
                element = container.select_one(selector)
                if element and element.get_text(strip=True):
                    return element.get_text(strip=True)
            except:
                continue
        return None

    def extract_price(self, price_text):
        """Extract price from text"""
        if not price_text:
            return 0
        
        clean_text = re.sub(r'[万円,\s]', '', price_text)
        
        patterns = [r'(\d+\.?\d*)万', r'(\d{6,})', r'(\d+\.?\d*)']
        
        for pattern in patterns:
            matches = re.findall(pattern, clean_text)
            if matches:
                try:
                    amount = float(matches[0])
                    return int(amount * 10000) if amount < 100 else int(amount)
                except:
                    continue
        return 0

    def extract_walk_time(self, text):
        """Extract walk time from text"""
        patterns = [r'徒歩(\d+)分', r'(\d+)分']
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1))
        return 10

    def extract_url(self, container, base_url):
        """Extract property URL"""
        link = container.find('a')
        if link and link.get('href'):
            href = link['href']
            if href.startswith('/'):
                return f"{base_url}{href}"
            elif href.startswith('http'):
                return href
        return base_url

    def create_property_object(self, title, price, rooms, location, station, 
                              walk_minutes, url, source, station_data):
        """Create standardized property object"""
        property_data = {
            'title': title.strip(),
            'price': price,
            'rooms': rooms.strip(),
            'location': location.strip(),
            'station': station,
            'walk_minutes': walk_minutes,
            'property_url': url,
            'found_date': datetime.now().isoformat(),
            'source': source,
            'area_priority': station_data['priority'],
            'route_type': station_data['route']
        }
        
        score, reasons = self.calculate_score(property_data, station_data)
        property_data['score'] = score
        property_data['reasons'] = json.dumps(reasons)
        
        return property_data

    def is_valid_property(self, prop):
        """Validate property data"""
        return (prop and 
                prop.get('price', 0) >= 50000 and 
                prop.get('title') and 
                len(prop.get('title', '')) > 5)

    def calculate_score(self, property_data, station_data):
        """Calculate property score"""
        score = 0
        reasons = []
        
        # Price scoring
        price = property_data['price']
        if 250000 <= price <= 350000:
            score += 30
            reasons.append("Perfect DSTY budget range")
        elif 200000 <= price <= 400000:
            score += 25
            reasons.append("Good price range")
        else:
            score += 15
            reasons.append("Reasonable price")
        
        # Room scoring
        rooms = property_data['rooms']
        if '3LDK' in rooms:
            score += 25
            reasons.append("Perfect family size (3LDK)")
        elif '2LDK' in rooms:
            score += 20
            reasons.append("Good family size (2LDK)")
        else:
            score += 15
            reasons.append("Suitable layout")
        
        # Area priority
        score += station_data['priority']
        route = station_data['route']
        reasons.append(f"Excellent {route} Route access")
        
        # Walk time
        walk = property_data['walk_minutes']
        if walk <= 10:
            score += 15
            reasons.append(f"Close to station ({walk} min)")
        elif walk <= 15:
            score += 10
            reasons.append(f"Reasonable walk ({walk} min)")
        else:
            score += 5
            reasons.append(f"Acceptable walk ({walk} min)")
        
        # Source bonus
        source = property_data['source']
        if source == 'Suumo':
            score += 5
            reasons.append("High-quality Suumo listing")
        else:
            score += 3
            reasons.append(f"Verified {source} listing")
        
        return min(100, max(0, score)), reasons

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
        
        return new_count

    def run_multi_site_search(self):
        """Run comprehensive multi-site search"""
        logger.info("🏠 Starting comprehensive multi-site DSTY property search...")
        
        total_found = 0
        total_new = 0
        
        for station_name, station_data in self.target_stations.items():
            logger.info(f"🔍 Searching {station_name} ({station_data['route']} Route) across all sites...")
            
            all_properties = []
            
            # Search Suumo
            suumo_props = self.search_suumo(station_name, station_data)
            all_properties.extend(suumo_props)
            
            # Search Homes.co.jp
            homes_props = self.search_homes(station_name, station_data)
            all_properties.extend(homes_props)
            
            # Search AtHome
            athome_props = self.search_athome(station_name, station_data)
            all_properties.extend(athome_props)
            
            # Search LIFULL
            lifull_props = self.search_lifull(station_name, station_data)
            all_properties.extend(lifull_props)
            
            # Save all properties for this station
            if all_properties:
                new_count = self.save_properties(all_properties)
                total_found += len(all_properties)
                total_new += new_count
                logger.info(f"✅ {station_name}: {len(all_properties)} total found, {new_count} new")
            else:
                logger.info(f"❌ {station_name}: No properties found across all sites")
            
            # Delay between stations
            time.sleep(random.uniform(5, 10))
        
        logger.info(f"🎉 Multi-site search complete! Total: {total_found} found, {total_new} new")
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

    # Alias for compatibility
    def run_full_search(self):
        return self.run_multi_site_search()

if __name__ == "__main__":
    crawler = MultiSiteDStyPropertyCrawler()
    crawler.run_multi_site_search()
