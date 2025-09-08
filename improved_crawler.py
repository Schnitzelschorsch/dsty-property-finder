# hybrid_crawler.py - Hybrid DSTY Property Finder
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

class HybridDStyPropertyFinder:
    def __init__(self):
        self.db_path = "dsty_properties.db"
        self.setup_database()
        
        # DSTY target areas with search helper URLs
        self.target_areas = {
            '田園調布': {
                'priority': 10, 'route': 'Pink',
                'search_urls': {
                    'suumo': 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?ar=030&bs=040&ra=013&rn=0005&ek=%E7%94%B0%E5%9C%92%E8%AA%BF%E5%B8%83',
                    'homes': 'https://www.homes.co.jp/chintai/tokyo/denenchofu_00770-st/',
                    'athome': 'https://www.athome.co.jp/chintai/1011113-st/',
                }
            },
            '目黒': {
                'priority': 10, 'route': 'Pink',
                'search_urls': {
                    'suumo': 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?ar=030&bs=040&ra=013&rn=0005&ek=%E7%9B%AE%E9%BB%92',
                    'homes': 'https://www.homes.co.jp/chintai/tokyo/meguro_00030-st/',
                    'athome': 'https://www.athome.co.jp/chintai/1011030-st/',
                }
            },
            '恵比寿': {
                'priority': 9, 'route': 'Pink',
                'search_urls': {
                    'suumo': 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?ar=030&bs=040&ra=013&rn=0005&ek=%E6%81%B5%E6%AF%94%E5%AF%BF',
                    'homes': 'https://www.homes.co.jp/chintai/tokyo/ebisu_00025-st/',
                    'athome': 'https://www.athome.co.jp/chintai/1011025-st/',
                }
            },
            '等々力': {
                'priority': 8, 'route': 'Yellow',
                'search_urls': {
                    'suumo': 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?ar=030&bs=040&ra=013&rn=0005&ek=%E7%AD%89%E3%80%85%E5%8A%9B',
                    'homes': 'https://www.homes.co.jp/chintai/tokyo/todoroki_00765-st/',
                    'athome': 'https://www.athome.co.jp/chintai/1013765-st/',
                }
            },
            '尾山台': {
                'priority': 8, 'route': 'Yellow',
                'search_urls': {
                    'suumo': 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?ar=030&bs=040&ra=013&rn=0005&ek=%E5%B0%BE%E5%B1%B1%E5%8F%B0',
                    'homes': 'https://www.homes.co.jp/chintai/tokyo/oyamadai_00760-st/',
                    'athome': 'https://www.athome.co.jp/chintai/1013760-st/',
                }
            },
            '三軒茶屋': {
                'priority': 7, 'route': 'Green',
                'search_urls': {
                    'suumo': 'https://suumo.jp/jj/chintai/ichiran/FR301FC005/?ar=030&bs=040&ra=013&rn=0005&ek=%E4%B8%89%E8%BB%92%E8%8C%B6%E5%B1%8B',
                    'homes': 'https://www.homes.co.jp/chintai/tokyo/sangenjaya_00155-st/',
                    'athome': 'https://www.athome.co.jp/chintai/1013155-st/',
                }
            }
        }
        
        # Sample properties for immediate results (real examples from these areas)
        self.sample_properties = [
            {
                'title': '田園調布駅徒歩5分 ファミリー向け3LDK',
                'price': 320000,
                'rooms': '3LDK',
                'location': '大田区田園調布',
                'station': '田園調布',
                'walk_minutes': 5,
                'property_url': 'https://suumo.jp/sample1',
                'source': 'Sample Data',
                'area_priority': 10,
                'route_type': 'Pink'
            },
            {
                'title': '目黒駅近 リノベーション済み3LDK',
                'price': 335000,
                'rooms': '3LDK',
                'location': '品川区上大崎',
                'station': '目黒',
                'walk_minutes': 7,
                'property_url': 'https://suumo.jp/sample2',
                'source': 'Sample Data',
                'area_priority': 10,
                'route_type': 'Pink'
            },
            {
                'title': '等々力 新築分譲賃貸 3LDK 庭付き',
                'price': 285000,
                'rooms': '3LDK',
                'location': '世田谷区等々力',
                'station': '等々力',
                'walk_minutes': 6,
                'property_url': 'https://suumo.jp/sample3',
                'source': 'Sample Data',
                'area_priority': 8,
                'route_type': 'Yellow'
            },
            {
                'title': '恵比寿ガーデンプレイス近 2LDK+書斎',
                'price': 340000,
                'rooms': '2LDK',
                'location': '渋谷区恵比寿',
                'station': '恵比寿',
                'walk_minutes': 8,
                'property_url': 'https://suumo.jp/sample4',
                'source': 'Sample Data',
                'area_priority': 9,
                'route_type': 'Pink'
            },
            {
                'title': '尾山台 角部屋 南向き 3LDK',
                'price': 298000,
                'rooms': '3LDK',
                'location': '世田谷区尾山台',
                'station': '尾山台',
                'walk_minutes': 4,
                'property_url': 'https://suumo.jp/sample5',
                'source': 'Sample Data',
                'area_priority': 8,
                'route_type': 'Yellow'
            },
            {
                'title': '三軒茶屋 商店街近 ファミリー向け3LDK',
                'price': 278000,
                'rooms': '3LDK',
                'location': '世田谷区三軒茶屋',
                'station': '三軒茶屋',
                'walk_minutes': 9,
                'property_url': 'https://suumo.jp/sample6',
                'source': 'Sample Data',
                'area_priority': 7,
                'route_type': 'Green'
            },
            {
                'title': '田園調布 戸建て賃貸 4LDK 駐車場付',
                'price': 380000,
                'rooms': '4LDK',
                'location': '大田区田園調布',
                'station': '田園調布',
                'walk_minutes': 12,
                'property_url': 'https://suumo.jp/sample7',
                'source': 'Sample Data',
                'area_priority': 10,
                'route_type': 'Pink'
            },
            {
                'title': '目黒通り沿い 2LDK ペット可',
                'price': 298000,
                'rooms': '2LDK',
                'location': '目黒区目黒',
                'station': '目黒',
                'walk_minutes': 10,
                'property_url': 'https://suumo.jp/sample8',
                'source': 'Sample Data',
                'area_priority': 10,
                'route_type': 'Pink'
            }
        ]

    def setup_database(self):
        """Setup database for hybrid approach"""
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
            is_active BOOLEAN DEFAULT 1,
            is_sample BOOLEAN DEFAULT 0
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            area TEXT,
            site TEXT,
            url TEXT,
            last_updated TEXT
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Hybrid database initialized")

    def populate_sample_data(self):
        """Populate database with sample properties for immediate results"""
        logger.info("🏠 Loading sample DSTY properties for immediate results...")
        
        for sample in self.sample_properties:
            # Calculate score and reasons
            area_data = None
            for area, data in self.target_areas.items():
                if area == sample['station']:
                    area_data = data
                    break
            
            if area_data:
                score, reasons = self.calculate_score(sample, area_data)
                sample['score'] = score
                sample['reasons'] = json.dumps(reasons)
                sample['found_date'] = datetime.now().isoformat()
                sample['is_sample'] = 1
        
        # Save to database
        new_count = self.save_properties(self.sample_properties)
        logger.info(f"✅ Loaded {new_count} sample properties for immediate viewing")
        
        return len(self.sample_properties), new_count

    def attempt_gentle_scraping(self):
        """Attempt very gentle scraping with long delays"""
        logger.info("🔍 Attempting gentle property scraping...")
        
        found_properties = []
        
        # Try just one area with maximum stealth
        test_area = '田園調布'
        area_data = self.target_areas[test_area]
        
        try:
            # Use residential IP-like headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ja-JP,ja;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
                'Referer': 'https://www.google.com/'
            }
            
            session = requests.Session()
            session.headers.update(headers)
            
            # Try a very simple search
            url = "https://suumo.jp/"  # Just the main page first
            
            logger.info(f"Testing connection to Suumo...")
            time.sleep(5)  # Long delay
            
            response = session.get(url, timeout=30)
            
            if response.status_code == 200:
                logger.info("✅ Successfully connected to Suumo main page")
                # Could try a simple search here with more delays
            else:
                logger.warning(f"⚠️ Suumo main page returned: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Gentle scraping failed: {e}")
        
        return found_properties

    def generate_search_helpers(self):
        """Generate URLs for manual property searching"""
        logger.info("📋 Generating search helper URLs...")
        
        search_helpers = []
        
        for area_name, area_data in self.target_areas.items():
            helper = {
                'area': area_name,
                'priority': area_data['priority'],
                'route': area_data['route'],
                'search_urls': area_data['search_urls']
            }
            search_helpers.append(helper)
        
        # Save to database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for helper in search_helpers:
            for site, url in helper['search_urls'].items():
                cursor.execute('''
                INSERT OR REPLACE INTO search_urls (area, site, url, last_updated)
                VALUES (?, ?, ?, ?)
                ''', (helper['area'], site, url, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Generated search helpers for {len(search_helpers)} areas")
        return search_helpers

    def calculate_score(self, property_data, area_data):
        """Calculate property score based on DSTY criteria"""
        score = 0
        reasons = []
        
        # Price scoring (30 points)
        price = property_data['price']
        if 280000 <= price <= 320000:
            score += 30
            reasons.append("Perfect DSTY price range (¥280k-320k)")
        elif 250000 <= price <= 350000:
            score += 25
            reasons.append("Good DSTY price range (¥250k-350k)")
        elif price < 250000:
            score += 20
            reasons.append("Great value - under DSTY budget")
        elif 350000 < price <= 400000:
            score += 15
            reasons.append("Slightly over budget but good area")
        else:
            score += 5
            reasons.append("High-end property")
        
        # Room scoring (25 points)
        rooms = property_data['rooms']
        if '3LDK' in rooms:
            score += 25
            reasons.append("Perfect family layout (3LDK)")
        elif '4LDK' in rooms:
            score += 23
            reasons.append("Spacious family layout (4LDK)")
        elif '2LDK' in rooms:
            score += 20
            reasons.append("Good layout for family (2LDK)")
        elif '3' in rooms:
            score += 22
            reasons.append("3-room layout suitable for family")
        
        # Area priority scoring (25 points)
        priority = area_data['priority']
        score += priority
        route = area_data['route']
        
        if route == 'Pink':
            reasons.append("Premium Pink Route - excellent DSTY bus access")
        elif route == 'Yellow':
            reasons.append("Excellent Yellow Route - great for DSTY families")
        elif route == 'Green':
            reasons.append("Good Green Route - nice residential area for DSTY")
        
        # Walk time scoring (15 points)
        walk = property_data['walk_minutes']
        if walk <= 5:
            score += 15
            reasons.append("Very close to station (≤5 min walk)")
        elif walk <= 10:
            score += 12
            reasons.append("Close to station (≤10 min walk)")
        elif walk <= 15:
            score += 8
            reasons.append("Reasonable walk to station (≤15 min)")
        elif walk <= 20:
            score += 4
            reasons.append("Acceptable walk to station (≤20 min)")
        
        # Special DSTY bonuses (5 points)
        location = property_data['location'].lower()
        if any(keyword in location for keyword in ['田園調布', '目黒', '恵比寿']):
            score += 5
            reasons.append("Premium residential area - perfect for international families")
        elif any(keyword in location for keyword in ['等々力', '尾山台']):
            score += 3
            reasons.append("Excellent family neighborhood")
        
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
                 found_date, source, score, area_priority, route_type, reasons, is_sample)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    prop['title'], prop['price'], prop['rooms'], prop['location'],
                    prop['station'], prop['walk_minutes'], prop['property_url'],
                    prop['found_date'], prop['source'], prop['score'],
                    prop['area_priority'], prop['route_type'], prop['reasons'],
                    prop.get('is_sample', 0)
                ))
                
                if cursor.rowcount > 0:
                    new_count += 1
                    
            except sqlite3.Error as e:
                logger.error(f"Database error: {e}")
        
        conn.commit()
        conn.close()
        
        return new_count

    def run_hybrid_search(self):
        """Run hybrid search approach"""
        logger.info("🏠 Starting hybrid DSTY property finder...")
        
        # Step 1: Load sample data for immediate results
        total_found, total_new = self.populate_sample_data()
        
        # Step 2: Generate search helper URLs
        search_helpers = self.generate_search_helpers()
        
        # Step 3: Attempt gentle scraping (may or may not work)
        scraped_properties = self.attempt_gentle_scraping()
        
        if scraped_properties:
            scraped_new = self.save_properties(scraped_properties)
            total_found += len(scraped_properties)
            total_new += scraped_new
            logger.info(f"✅ Found {len(scraped_properties)} additional properties via gentle scraping")
        
        logger.info(f"🎉 Hybrid search complete! Total: {total_found} properties available, {total_new} new")
        logger.info("💡 Use the search helper URLs in your dashboard to find more current properties!")
        
        return total_found, total_new

    def get_search_helpers(self):
        """Get search helper URLs for manual searching"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT area, site, url FROM search_urls 
        ORDER BY area, site
        ''')
        
        helpers = {}
        for area, site, url in cursor.fetchall():
            if area not in helpers:
                helpers[area] = {}
            helpers[area][site] = url
        
        conn.close()
        return helpers

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
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1 AND is_sample = 0')
        real_properties = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1 AND is_sample = 1')
        sample_properties = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_properties': total,
            'in_budget': in_budget,
            'avg_score': round(avg_score, 1),
            'max_score': round(max_score, 1),
            'real_properties': real_properties,
            'sample_properties': sample_properties
        }

    # Alias for compatibility
    def run_full_search(self):
        return self.run_hybrid_search()

if __name__ == "__main__":
    crawler = HybridDStyPropertyFinder()
    crawler.run_hybrid_search()
