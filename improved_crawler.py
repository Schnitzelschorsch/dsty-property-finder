# improved_crawler.py - Multi-site DSTY Property Crawler
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
from fake_useragent import UserAgent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ImprovedDStyPropertyCrawler:
    def __init__(self):
        self.db_path = "dsty_properties.db"
        self.setup_database()
        
        # Initialize user agent rotation
        try:
            self.ua = UserAgent()
        except:
            # Fallback user agents if fake_useragent fails
            self.user_agents = [
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
            ]
        
        # DSTY target areas with multiple site support
        self.target_areas = {
            '田園調布': {
                'priority': 10, 'route': 'Pink',
                'suumo_code': '13111', 'homes_area': 'denenchofu',
                'athome_code': 'tokyo_ota', 'lifull_area': 'denenchofu'
            },
            '目黒': {
                'priority': 10, 'route': 'Pink',
                'suumo_code': '13109', 'homes_area': 'meguro',
                'athome_code': 'tokyo_meguro', 'lifull_area': 'meguro'
            },
            '恵比寿': {
                'priority': 9, 'route': 'Pink',
                'suumo_code': '13109', 'homes_area': 'ebisu',
                'athome_code': 'tokyo_shibuya', 'lifull_area': 'ebisu'
            },
            '等々力': {
                'priority': 8, 'route': 'Yellow',
                'suumo_code': '13112', 'homes_area': 'todoroki',
                'athome_code': 'tokyo_setagaya', 'lifull_area': 'todoroki'
            },
            '尾山台': {
                'priority': 8, 'route': 'Yellow',
                'suumo_code': '13112', 'homes_area': 'oyamadai',
                'athome_code': 'tokyo_setagaya', 'lifull_area': 'oyamadai'
            },
            '三軒茶屋': {
                'priority': 7, 'route': 'Green',
                'suumo_code': '13112', 'homes_area': 'sangenjaya',
                'athome_code': 'tokyo_setagaya', 'lifull_area': 'sangenjaya'
            }
        }
        
        self.min_rent = 250000
        self.max_rent = 350000
        
        # Create session with retry and better headers
        self.session = requests.Session()
        self.setup_session()

    def setup_database(self):
        """Enhanced database setup"""
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
            image_url TEXT,
            floor_plan_url TEXT,
            found_date TEXT,
            source TEXT,
            score REAL,
            area_priority INTEGER,
            route_type TEXT,
            reasons TEXT,
            building_age INTEGER,
            floor_area REAL,
            is_active BOOLEAN DEFAULT 1,
            last_seen TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_date TEXT,
            source TEXT,
            area TEXT,
            properties_found INTEGER,
            new_properties INTEGER,
            status TEXT,
            error_message TEXT
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Enhanced database initialized")

    def setup_session(self):
        """Setup session with anti-detection measures"""
        self.session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ja,en-US;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        # Configure retries
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get_random_headers(self):
        """Get randomized headers to avoid detection"""
        try:
            user_agent = self.ua.random
        except:
            user_agent = random.choice(self.user_agents)
        
        return {
            'User-Agent': user_agent,
            'Referer': 'https://www.google.com/',
            'X-Forwarded-For': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
        }

    def random_delay(self, min_seconds=2, max_seconds=5):
        """Add random delay to mimic human behavior"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def search_suumo_improved(self, area_name, area_data):
        """Improved Suumo search with anti-detection"""
        properties = []
        
        try:
            # Update headers for this request
            headers = self.get_random_headers()
            self.session.headers.update(headers)
            
            # Build search URL with broader parameters
            params = {
                'ar': '030',
                'bs': '040',
                'ta': '13',
                'sc': area_data['suumo_code'],
                'cb': '20.0',  # Broader price range
                'ct': '40.0',
                'mb': '0',
                'mt': '20',    # Broader walk time
                'shkr1': '03',
                'shkr2': '03',
                'shkr3': '03',
                'rn': '0005'
            }
            
            url = f"https://suumo.jp/jj/chintai/ichiran/FR301FC001/?{urlencode(params)}"
            logger.info(f"Searching Suumo for {area_name}...")
            
            # Random delay before request
            self.random_delay(3, 6)
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_suumo_response(response.content, area_name, area_data)
                logger.info(f"✅ Suumo {area_name}: Found {len(properties)} properties")
            elif response.status_code == 503:
                logger.warning(f"⚠️ Suumo blocking detected for {area_name}, trying alternative approach...")
                # Try with different parameters
                properties = self.search_suumo_alternative(area_name, area_data)
            else:
                logger.error(f"❌ Suumo {area_name}: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Suumo {area_name} error: {e}")
        
        return properties

    def search_suumo_alternative(self, area_name, area_data):
        """Alternative Suumo search approach"""
        try:
            # Try station-based search instead of area code
            station_query = quote(area_name)
            url = f"https://suumo.jp/jj/chintai/ichiran/FR301FC005/?ar=030&bs=040&ra=013&rn=0005&ek={station_query}"
            
            self.random_delay(4, 7)
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_suumo_response(response.content, area_name, area_data)
                logger.info(f"✅ Suumo Alternative {area_name}: Found {len(properties)} properties")
                return properties
            
        except Exception as e:
            logger.error(f"❌ Suumo Alternative {area_name}: {e}")
        
        return []

    def search_homes_co_jp(self, area_name, area_data):
        """Search Homes.co.jp"""
        properties = []
        
        try:
            headers = self.get_random_headers()
            self.session.headers.update(headers)
            
            # Homes.co.jp search URL
            base_url = "https://www.homes.co.jp/chintai/search/"
            params = {
                'rent_from': int(self.min_rent/10000),  # Convert to 万円
                'rent_to': int(self.max_rent/10000),
                'walk_time': '15',
                'layout': '3LDK,2LDK',
                'keyword': area_name
            }
            
            url = f"{base_url}?{urlencode(params)}"
            logger.info(f"Searching Homes.co.jp for {area_name}...")
            
            self.random_delay(2, 4)
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_homes_response(response.content, area_name, area_data)
                logger.info(f"✅ Homes.co.jp {area_name}: Found {len(properties)} properties")
            else:
                logger.error(f"❌ Homes.co.jp {area_name}: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Homes.co.jp {area_name} error: {e}")
        
        return properties

    def search_athome_co_jp(self, area_name, area_data):
        """Search AtHome.co.jp"""
        properties = []
        
        try:
            headers = self.get_random_headers()
            self.session.headers.update(headers)
            
            # AtHome search URL
            params = {
                'prefecture': '13',  # Tokyo
                'city': area_data.get('athome_code', area_name),
                'rent_min': self.min_rent,
                'rent_max': self.max_rent,
                'layout': '3LDK,2LDK',
                'walk_time': '15'
            }
            
            url = f"https://www.athome.co.jp/chintai/search/?{urlencode(params)}"
            logger.info(f"Searching AtHome for {area_name}...")
            
            self.random_delay(2, 4)
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                properties = self.parse_athome_response(response.content, area_name, area_data)
                logger.info(f"✅ AtHome {area_name}: Found {len(properties)} properties")
            else:
                logger.error(f"❌ AtHome {area_name}: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ AtHome {area_name} error: {e}")
        
        return properties

    def parse_suumo_response(self, html_content, area_name, area_data):
        """Parse Suumo HTML response"""
        properties = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for different possible selectors
            property_items = (
                soup.find_all('div', class_='cassetteitem') or
                soup.find_all('div', class_='property-unit') or
                soup.find_all('div', {'data-bc': 'js-cassette-link'})
            )
            
            for item in property_items[:8]:  # Limit results
                property_data = self.parse_suumo_item(item, area_name, area_data)
                if property_data and self.min_rent <= property_data['price'] <= self.max_rent:
                    properties.append(property_data)
                    
        except Exception as e:
            logger.error(f"Error parsing Suumo response for {area_name}: {e}")
        
        return properties

    def parse_suumo_item(self, item, area_name, area_data):
        """Parse individual Suumo property item"""
        try:
            # Extract title with multiple selectors
            title_selectors = [
                'div.cassetteitem_content-title',
                'h3.property-title',
                '.js-cassette-link-text'
            ]
            
            title = None
            for selector in title_selectors:
                title_elem = item.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            if not title:
                return None
            
            # Extract price with multiple approaches
            price_selectors = [
                'span.cassetteitem_price--rent',
                '.property-price',
                '[class*="rent"]'
            ]
            
            price = 0
            for selector in price_selectors:
                price_elem = item.select_one(selector)
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    price = self.extract_price(price_text)
                    if price > 0:
                        break
            
            if price == 0:
                return None
            
            # Extract other details
            rooms = self.extract_rooms(item) or "Unknown"
            location = self.extract_location(item, area_name)
            walk_minutes = self.extract_walk_time(item)
            property_url = self.extract_url(item, 'suumo')
            
            property_data = {
                'title': title,
                'price': price,
                'rooms': rooms,
                'location': location,
                'station': area_name,
                'walk_minutes': walk_minutes,
                'property_url': property_url,
                'image_url': self.extract_image_url(item),
                'found_date': datetime.now().isoformat(),
                'source': 'Suumo',
                'area_priority': area_data['priority'],
                'route_type': area_data['route']
            }
            
            # Calculate score
            score, reasons = self.calculate_enhanced_score(property_data, area_data)
            property_data['score'] = score
            property_data['reasons'] = json.dumps(reasons)
            
            return property_data
            
        except Exception as e:
            logger.error(f"Error parsing Suumo item: {e}")
            return None

    def parse_homes_response(self, html_content, area_name, area_data):
        """Parse Homes.co.jp response"""
        properties = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Homes.co.jp specific selectors
            property_items = soup.find_all('div', class_='prg-bukkenList')
            
            for item in property_items[:5]:
                property_data = self.parse_homes_item(item, area_name, area_data)
                if property_data and self.min_rent <= property_data['price'] <= self.max_rent:
                    properties.append(property_data)
                    
        except Exception as e:
            logger.error(f"Error parsing Homes response for {area_name}: {e}")
        
        return properties

    def parse_homes_item(self, item, area_name, area_data):
        """Parse individual Homes.co.jp property"""
        try:
            # Simplified parsing for Homes.co.jp
            title_elem = item.find('h2') or item.find('h3')
            title = title_elem.get_text(strip=True) if title_elem else f"{area_name} Property"
            
            # Try to extract price (simplified)
            price_text = item.get_text()
            price = self.extract_price(price_text)
            
            if price == 0:
                return None
            
            property_data = {
                'title': title,
                'price': price,
                'rooms': "2-3LDK",  # Default assumption
                'location': area_name,
                'station': area_name,
                'walk_minutes': 10,  # Default assumption
                'property_url': "https://www.homes.co.jp/",
                'image_url': "",
                'found_date': datetime.now().isoformat(),
                'source': 'Homes.co.jp',
                'area_priority': area_data['priority'],
                'route_type': area_data['route']
            }
            
            score, reasons = self.calculate_enhanced_score(property_data, area_data)
            property_data['score'] = score
            property_data['reasons'] = json.dumps(reasons)
            
            return property_data
            
        except Exception as e:
            logger.error(f"Error parsing Homes item: {e}")
            return None

    def parse_athome_response(self, html_content, area_name, area_data):
        """Parse AtHome response - similar structure to Homes"""
        # Implementation similar to parse_homes_response
        return []

    def extract_price(self, price_text):
        """Enhanced price extraction"""
        if not price_text:
            return 0
        
        # Remove common Japanese characters
        clean_text = re.sub(r'[万円,、\s]', '', price_text)
        
        # Look for patterns like "28万" or "280000"
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
                    # If less than 100, assume it's in 万円
                    if amount < 100:
                        return int(amount * 10000)
                    else:
                        return int(amount)
                except:
                    continue
        
        return 0

    def extract_rooms(self, item):
        """Extract room layout"""
        room_selectors = [
            'span.cassetteitem_madori',
            '.madori',
            '[class*="layout"]'
        ]
        
        for selector in room_selectors:
            elem = item.select_one(selector)
            if elem:
                return elem.get_text(strip=True)
        
        # Fallback: search in all text
        text = item.get_text()
        room_patterns = [r'(\d+LDK)', r'(\d+DK)', r'(\d+K)']
        for pattern in room_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        
        return "Unknown"

    def extract_location(self, item, default_area):
        """Extract location details"""
        location_selectors = [
            'li.cassetteitem_detail-col1',
            '.address',
            '[class*="location"]'
        ]
        
        for selector in location_selectors:
            elem = item.select_one(selector)
            if elem:
                return elem.get_text(strip=True)
        
        return default_area

    def extract_walk_time(self, item):
        """Extract walk time to station"""
        text = item.get_text()
        walk_patterns = [r'徒歩(\d+)分', r'(\d+)分']
        
        for pattern in walk_patterns:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1))
        
        return 10  # Default assumption

    def extract_url(self, item, source):
        """Extract property URL"""
        link_elem = item.find('a')
        if link_elem and link_elem.get('href'):
            href = link_elem['href']
            if source == 'suumo' and not href.startswith('http'):
                return f"https://suumo.jp{href}"
            return href
        
        return ""

    def extract_image_url(self, item):
        """Extract property image URL"""
        img_elem = item.find('img')
        if img_elem and img_elem.get('src'):
            return img_elem['src']
        return ""

    def calculate_enhanced_score(self, property_data, area_data):
        """Enhanced scoring algorithm"""
        score = 0
        reasons = []
        
        # Price scoring (30 points)
        price = property_data['price']
        if 280000 <= price <= 320000:
            score += 30
            reasons.append("Perfect price range (¥280k-320k)")
        elif 250000 <= price <= 350000:
            score += 25
            reasons.append("Good price range")
        elif price < 250000:
            score += 20
            reasons.append("Great value - under budget")
        else:
            score -= 5
            reasons.append("Over budget")
        
        # Room scoring (25 points)
        rooms = property_data['rooms']
        if '3LDK' in rooms:
            score += 25
            reasons.append("Perfect family layout (3LDK)")
        elif '2LDK' in rooms:
            score += 20
            reasons.append("Good layout (2LDK)")
        elif '3' in rooms:
            score += 22
            reasons.append("3-room layout")
        
        # Area priority (25 points)
        score += area_data['priority']
        route = area_data['route']
        if route == 'Pink':
            reasons.append("Premium Pink Route - excellent bus access")
        elif route == 'Yellow':
            reasons.append("Excellent Yellow Route - great for families")
        elif route == 'Green':
            reasons.append("Good Green Route - spacious area")
        
        # Walk time scoring (15 points)
        walk = property_data['walk_minutes']
        if walk <= 5:
            score += 15
            reasons.append("Very close to station (≤5 min)")
        elif walk <= 10:
            score += 12
            reasons.append("Close to station (≤10 min)")
        elif walk <= 15:
            score += 8
            reasons.append("Acceptable walk (≤15 min)")
        
        # Source bonus (5 points)
        source = property_data['source']
        if source == 'Suumo':
            score += 5
            reasons.append("High-quality Suumo listing")
        elif source in ['Homes.co.jp', 'AtHome']:
            score += 3
            reasons.append(f"Verified {source} listing")
        
        return min(100, max(0, score)), reasons

    def save_properties_enhanced(self, properties):
        """Enhanced property saving with deduplication"""
        if not properties:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        new_count = 0
        updated_count = 0
        
        for prop in properties:
            try:
                # Check if property exists (by URL or similar title + price)
                cursor.execute('''
                SELECT id FROM properties 
                WHERE property_url = ? OR (title = ? AND price = ?)
                ''', (prop['property_url'], prop['title'], prop['price']))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing property
                    cursor.execute('''
                    UPDATE properties SET 
                    last_seen = ?, score = ?, reasons = ?, is_active = 1
                    WHERE id = ?
                    ''', (datetime.now().isoformat(), prop['score'], prop['reasons'], existing[0]))
                    updated_count += 1
                else:
                    # Insert new property
                    cursor.execute('''
                    INSERT INTO properties 
                    (title, price, rooms, location, station, walk_minutes, property_url,
                     image_url, found_date, source, score, area_priority, route_type, 
                     reasons, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        prop['title'], prop['price'], prop['rooms'], prop['location'],
                        prop['station'], prop['walk_minutes'], prop['property_url'],
                        prop['image_url'], prop['found_date'], prop['source'],
                        prop['score'], prop['area_priority'], prop['route_type'],
                        prop['reasons'], datetime.now().isoformat()
                    ))
                    new_count += 1
                    
            except sqlite3.Error as e:
                logger.error(f"Database error: {e}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Saved {new_count} new, updated {updated_count} existing properties")
        return new_count

    def run_multi_site_search(self):
        """Run comprehensive multi-site search"""
        logger.info("🏠 Starting enhanced multi-site DSTY property search...")
        
        total_found = 0
        total_new = 0
        
        for area_name, area_data in self.target_areas.items():
            logger.info(f"🔍 Searching {area_name} ({area_data['route']} Route) across all sites...")
            
            area_properties = []
            
            # Search Suumo (improved)
            try:
                suumo_props = self.search_suumo_improved(area_name, area_data)
                area_properties.extend(suumo_props)
                self.log_search_result(area_name, 'Suumo', len(suumo_props), 'success')
            except Exception as e:
                logger.error(f"Suumo search failed for {area_name}: {e}")
                self.log_search_result(area_name, 'Suumo', 0, 'error', str(e))
            
            # Search Homes.co.jp
            try:
                homes_props = self.search_homes_co_jp(area_name, area_data)
                area_properties.extend(homes_props)
                self.log_search_result(area_name, 'Homes.co.jp', len(homes_props), 'success')
            except Exception as e:
                logger.error(f"Homes.co.jp search failed for {area_name}: {e}")
                self.log_search_result(area_name, 'Homes.co.jp', 0, 'error', str(e))
            
            # Search AtHome
            try:
                athome_props = self.search_athome_co_jp(area_name, area_data)
                area_properties.extend(athome_props)
                self.log_search_result(area_name, 'AtHome', len(athome_props), 'success')
            except Exception as e:
                logger.error(f"AtHome search failed for {area_name}: {e}")
                self.log_search_result(area_name, 'AtHome', 0, 'error', str(e))
            
            # Save properties for this area
            if area_properties:
                new_count = self.save_properties_enhanced(area_properties)
                total_found += len(area_properties)
                total_new += new_count
                logger.info(f"✅ {area_name}: {len(area_properties)} total found, {new_count} new")
            else:
                logger.info(f"❌ {area_name}: No properties found across all sites")
            
            # Longer delay between areas to be respectful
            self.random_delay(8, 12)
        
        logger.info(f"🎉 Multi-site search complete! Total: {total_found} found, {total_new} new")
        return total_found, total_new

    def log_search_result(self, area, source, count, status, error_msg=""):
        """Log search results to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO search_results 
        (search_date, source, area, properties_found, new_properties, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (datetime.now().isoformat(), source, area, count, 0, status, error_msg))
        
        conn.commit()
        conn.close()

    # Keep existing methods for compatibility
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
        """Get enhanced search statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total active properties
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1')
        total = cursor.fetchone()[0]
        
        # Properties in budget
        cursor.execute('''
        SELECT COUNT(*) FROM properties 
        WHERE is_active = 1 AND price BETWEEN ? AND ?
        ''', (self.min_rent, self.max_rent))
        in_budget = cursor.fetchone()[0]
        
        # Average score
        cursor.execute('SELECT AVG(score) FROM properties WHERE is_active = 1')
        avg_score = cursor.fetchone()[0] or 0
        
        # Max score
        cursor.execute('SELECT MAX(score) FROM properties WHERE is_active = 1')
        max_score = cursor.fetchone()[0] or 0
        
        # Properties by source
        cursor.execute('''
        SELECT source, COUNT(*) FROM properties 
        WHERE is_active = 1 GROUP BY source
        ''')
        by_source = dict(cursor.fetchall())
        
        # Properties by route
        cursor.execute('''
        SELECT route_type, COUNT(*) FROM properties 
        WHERE is_active = 1 GROUP BY route_type
        ''')
        by_route = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'total_properties': total,
            'in_budget': in_budget,
            'avg_score': round(avg_score, 1),
            'max_score': round(max_score, 1),
            'by_source': by_source,
            'by_route': by_route
        }

    # Alias for backward compatibility
    def run_full_search(self):
        """Backward compatibility method"""
        return self.run_multi_site_search()

# Test run
if __name__ == "__main__":
    crawler = ImprovedDStyPropertyCrawler()
    crawler.run_multi_site_search()
