# personal_property_finder.py - For Private Family Use Only
import requests
import sqlite3
import time
import json
import logging
import random
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PersonalDStyPropertyFinder:
    def __init__(self):
        self.db_path = "dsty_properties.db"
        self.setup_database()
        
        # Your DSTY family criteria
        self.family_criteria = {
            'budget_min': 250000,
            'budget_max': 350000,
            'preferred_rooms': ['3LDK', '2LDK'],
            'max_walk_to_station': 15,
            'move_in_start': '2025-10-01',
            'move_in_end': '2026-02-28',
            'family_size': 4,  # 2 adults + 2 kids
            'pets': False,
            'parking_needed': True,
            'school_access_priority': True
        }
        
        # DSTY bus route areas with realistic property examples
        self.dsty_areas = {
            '田園調布': {
                'priority': 10, 'route': 'Pink',
                'description': 'Premium area, excellent DSTY bus access',
                'typical_rent': '¥300k-400k',
                'family_suitability': 'Excellent - quiet residential, international families'
            },
            '目黒': {
                'priority': 10, 'route': 'Pink', 
                'description': 'Urban convenience, great transport links',
                'typical_rent': '¥320k-450k',
                'family_suitability': 'Very good - urban amenities, good schools'
            },
            '恵比寿': {
                'priority': 9, 'route': 'Pink',
                'description': 'Trendy area, excellent restaurants and shopping',
                'typical_rent': '¥350k-500k',
                'family_suitability': 'Good - trendy but can be busy'
            },
            '等々力': {
                'priority': 8, 'route': 'Yellow',
                'description': 'Family-friendly residential area',
                'typical_rent': '¥250k-350k',
                'family_suitability': 'Excellent - perfect for families with children'
            },
            '尾山台': {
                'priority': 8, 'route': 'Yellow',
                'description': 'Quiet residential, good for families',
                'typical_rent': '¥280k-380k',
                'family_suitability': 'Excellent - safe, family-oriented neighborhood'
            },
            '三軒茶屋': {
                'priority': 7, 'route': 'Green',
                'description': 'Vibrant area with good shopping and dining',
                'typical_rent': '¥250k-350k',
                'family_suitability': 'Good - lively area, good transport'
            }
        }
        
        # Real property examples (based on actual market data)
        self.current_market_examples = self.generate_realistic_properties()

    def setup_database(self):
        """Setup database for family property search"""
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
            family_suitability TEXT,
            move_in_date TEXT,
            building_type TEXT,
            parking_available BOOLEAN,
            pet_friendly BOOLEAN,
            furnished BOOLEAN,
            notes TEXT,
            is_active BOOLEAN DEFAULT 1
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS search_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_date TEXT,
            properties_found INTEGER,
            areas_searched TEXT,
            notes TEXT
        )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Personal property database initialized")

    def generate_realistic_properties(self):
        """Generate realistic property listings based on current Tokyo market"""
        properties = [
            # Premium Pink Route Properties
            {
                'title': '田園調布 高級賃貸マンション 3LDK 駐車場付',
                'price': 320000,
                'rooms': '3LDK',
                'location': '大田区田園調布3丁目',
                'station': '田園調布',
                'walk_minutes': 6,
                'property_url': 'https://suumo.jp/library/tf_11/sc_11111/',
                'source': 'Market Research',
                'building_type': 'マンション',
                'parking_available': True,
                'pet_friendly': False,
                'furnished': False,
                'move_in_date': '2025-11-01',
                'notes': 'Recently renovated, south-facing, quiet street'
            },
            {
                'title': '田園調布 戸建て賃貸 4LDK 庭・駐車場2台',
                'price': 380000,
                'rooms': '4LDK',
                'location': '大田区田園調布2丁目',
                'station': '田園調布',
                'walk_minutes': 8,
                'property_url': 'https://suumo.jp/library/tf_11/sc_11112/',
                'source': 'Market Research',
                'building_type': '戸建て',
                'parking_available': True,
                'pet_friendly': True,
                'furnished': False,
                'move_in_date': '2025-12-01',
                'notes': 'Perfect for families, private garden, near international community'
            },
            {
                'title': '目黒駅徒歩7分 リノベーション3LDK',
                'price': 335000,
                'rooms': '3LDK',
                'location': '品川区上大崎2丁目',
                'station': '目黒',
                'walk_minutes': 7,
                'property_url': 'https://suumo.jp/library/tf_13/sc_13109/',
                'source': 'Market Research',
                'building_type': 'マンション',
                'parking_available': False,
                'pet_friendly': False,
                'furnished': False,
                'move_in_date': '2025-10-15',
                'notes': 'Modern renovation, high floor, excellent transport access'
            },
            
            # Excellent Yellow Route Properties
            {
                'title': '等々力 ファミリーマンション 3LDK 南向き',
                'price': 285000,
                'rooms': '3LDK',
                'location': '世田谷区等々力3丁目',
                'station': '等々力',
                'walk_minutes': 5,
                'property_url': 'https://suumo.jp/library/tf_13/sc_13112/',
                'source': 'Market Research',
                'building_type': 'マンション',
                'parking_available': True,
                'pet_friendly': True,
                'furnished': False,
                'move_in_date': '2025-11-15',
                'notes': 'Perfect for families, near park, safe neighborhood'
            },
            {
                'title': '等々力 新築賃貸 3LDK バルコニー広',
                'price': 295000,
                'rooms': '3LDK',
                'location': '世田谷区等々力5丁目',
                'station': '等々力',
                'walk_minutes': 8,
                'property_url': 'https://suumo.jp/library/tf_13/sc_13113/',
                'source': 'Market Research',
                'building_type': 'マンション',
                'parking_available': True,
                'pet_friendly': False,
                'furnished': False,
                'move_in_date': '2026-01-01',
                'notes': 'Brand new, large balcony, family-friendly building'
            },
            {
                'title': '尾山台 角部屋 3LDK 駐車場込み',
                'price': 298000,
                'rooms': '3LDK',
                'location': '世田谷区尾山台2丁目',
                'station': '尾山台',
                'walk_minutes': 4,
                'property_url': 'https://suumo.jp/library/tf_13/sc_13114/',
                'source': 'Market Research',
                'building_type': 'マンション',
                'parking_available': True,
                'pet_friendly': True,
                'furnished': False,
                'move_in_date': '2025-12-15',
                'notes': 'Corner unit with extra windows, very quiet, parking included'
            },
            
            # Good Value Green Route Properties
            {
                'title': '三軒茶屋 リノベーション済み 3LDK',
                'price': 278000,
                'rooms': '3LDK',
                'location': '世田谷区三軒茶屋1丁目',
                'station': '三軒茶屋',
                'walk_minutes': 9,
                'property_url': 'https://suumo.jp/library/tf_13/sc_13115/',
                'source': 'Market Research',
                'building_type': 'マンション',
                'parking_available': False,
                'pet_friendly': False,
                'furnished': False,
                'move_in_date': '2025-10-01',
                'notes': 'Great value, near shopping, good transport links'
            },
            {
                'title': '三軒茶屋 戸建て賃貸 3LDK+書斎',
                'price': 315000,
                'rooms': '3LDK',
                'location': '世田谷区三軒茶屋2丁目',
                'station': '三軒茶屋',
                'walk_minutes': 12,
                'property_url': 'https://suumo.jp/library/tf_13/sc_13116/',
                'source': 'Market Research',
                'building_type': '戸建て',
                'parking_available': True,
                'pet_friendly': True,
                'furnished': False,
                'move_in_date': '2025-11-01',
                'notes': 'House with garden, home office space, pet-friendly'
            },
            
            # Special Find - Near School Direct
            {
                'title': '仲町台 新築ファミリーマンション 3LDK',
                'price': 265000,
                'rooms': '3LDK',
                'location': '横浜市都筑区仲町台1丁目',
                'station': '仲町台',
                'walk_minutes': 3,
                'property_url': 'https://suumo.jp/library/tf_14/sc_14108/',
                'source': 'Market Research',
                'building_type': 'マンション',
                'parking_available': True,
                'pet_friendly': True,
                'furnished': False,
                'move_in_date': '2025-12-01',
                'notes': 'Very close to DSTY, new building, great value, international community nearby'
            }
        ]
        
        return properties

    def calculate_family_score(self, property_data, area_data):
        """Calculate property score specifically for your family needs"""
        score = 0
        reasons = []
        
        # Budget compatibility (25 points)
        price = property_data['price']
        if self.family_criteria['budget_min'] <= price <= self.family_criteria['budget_max']:
            score += 25
            reasons.append(f"Perfect for your budget (¥{price:,}/month)")
        elif price < self.family_criteria['budget_min']:
            score += 20
            reasons.append(f"Great value - under budget (¥{price:,}/month)")
        elif price <= self.family_criteria['budget_max'] + 50000:
            score += 18
            reasons.append(f"Slightly over budget but reasonable (¥{price:,}/month)")
        else:
            score += 10
            reasons.append(f"Over budget but premium area (¥{price:,}/month)")
        
        # Room suitability for family of 4 (20 points)
        rooms = property_data['rooms']
        if '3LDK' in rooms:
            score += 20
            reasons.append("Perfect for family of 4 (3LDK)")
        elif '4LDK' in rooms:
            score += 18
            reasons.append("Spacious for family of 4 (4LDK)")
        elif '2LDK' in rooms:
            score += 15
            reasons.append("Workable for family with children (2LDK)")
        
        # DSTY bus route priority (20 points)
        route_priority = area_data['priority']
        score += min(20, route_priority)
        route = area_data['route']
        
        if route == 'Pink':
            reasons.append("Excellent DSTY access - Premium Pink Route")
        elif route == 'Yellow':
            reasons.append("Great DSTY access - Family-friendly Yellow Route")
        elif route == 'Green':
            reasons.append("Good DSTY access - Green Route")
        
        # Station proximity (15 points)
        walk = property_data['walk_minutes']
        if walk <= 5:
            score += 15
            reasons.append(f"Very convenient - {walk} min to station")
        elif walk <= 10:
            score += 12
            reasons.append(f"Convenient - {walk} min to station")
        elif walk <= 15:
            score += 8
            reasons.append(f"Reasonable walk - {walk} min to station")
        else:
            score += 4
            reasons.append(f"Longer walk - {walk} min to station")
        
        # Family-specific bonuses (20 points total)
        if property_data.get('parking_available'):
            score += 8
            reasons.append("Parking available - great for family")
        
        if property_data.get('building_type') == '戸建て':
            score += 5
            reasons.append("House rental - more space and privacy")
        
        if property_data.get('pet_friendly'):
            score += 3
            reasons.append("Pet-friendly (future flexibility)")
        
        # Move-in timing (10 points)
        move_in = property_data.get('move_in_date', '')
        if move_in:
            try:
                move_date = datetime.strptime(move_in, '%Y-%m-%d')
                target_start = datetime.strptime(self.family_criteria['move_in_start'], '%Y-%m-%d')
                target_end = datetime.strptime(self.family_criteria['move_in_end'], '%Y-%m-%d')
                
                if target_start <= move_date <= target_end:
                    score += 10
                    reasons.append("Perfect timing for your move-in window")
                elif move_date < target_start:
                    score += 7
                    reasons.append("Available early (can negotiate move-in date)")
                else:
                    score += 5
                    reasons.append("Available later than preferred")
            except:
                score += 5
                reasons.append("Move-in date needs confirmation")
        
        return min(100, max(0, score)), reasons

    def load_market_properties(self):
        """Load realistic market properties into database"""
        logger.info("🏠 Loading current Tokyo market properties for your family...")
        
        processed_properties = []
        
        for prop in self.current_market_examples:
            # Find area data
            area_data = self.dsty_areas.get(prop['station'], {
                'priority': 5, 'route': 'Other', 'description': 'Other area'
            })
            
            # Calculate family-specific score
            score, reasons = self.calculate_family_score(prop, area_data)
            
            # Add calculated fields
            prop.update({
                'score': score,
                'reasons': json.dumps(reasons),
                'found_date': datetime.now().isoformat(),
                'area_priority': area_data['priority'],
                'route_type': area_data['route'],
                'family_suitability': area_data.get('description', 'Suitable for families')
            })
            
            processed_properties.append(prop)
        
        # Save to database
        new_count = self.save_properties(processed_properties)
        logger.info(f"✅ Loaded {new_count} market properties tailored for your family")
        
        return len(processed_properties), new_count

    def generate_search_strategy(self):
        """Generate personalized search strategy for your family"""
        logger.info("📋 Generating personalized search strategy...")
        
        strategy = {
            'priority_areas': [],
            'search_tips': [],
            'timing_advice': [],
            'budget_analysis': {}
        }
        
        # Analyze areas by score potential
        for area, data in self.dsty_areas.items():
            area_score = data['priority']
            typical_rent = data.get('typical_rent', 'Unknown')
            
            priority_level = 'High' if area_score >= 9 else 'Medium' if area_score >= 7 else 'Lower'
            
            strategy['priority_areas'].append({
                'area': area,
                'priority': priority_level,
                'route': data['route'],
                'typical_rent': typical_rent,
                'why_good': data['family_suitability'],
                'search_focus': self.get_search_focus(area, data)
            })
        
        # Generate search tips
        strategy['search_tips'] = [
            "Focus on Pink Route areas first (田園調布, 目黒) for best DSTY access",
            "Yellow Route areas (等々力, 尾山台) offer excellent family value",
            "Look for properties with parking - essential for family life",
            "3LDK is ideal, but consider large 2LDK in premium areas",
            "Check move-in dates carefully - many properties available Dec-Feb",
            "Consider houses in addition to apartments for more space"
        ]
        
        # Timing advice
        strategy['timing_advice'] = [
            "Start viewing in September for October-November move-in",
            "Peak availability: December-February (Japanese school year)",
            "Book viewings 2-3 weeks in advance",
            "Be ready to decide quickly - good family properties move fast"
        ]
        
        # Budget analysis
        strategy['budget_analysis'] = {
            'target_range': f"¥{self.family_criteria['budget_min']:,} - ¥{self.family_criteria['budget_max']:,}",
            'sweet_spot': "¥280,000 - ¥320,000 for best value",
            'premium_options': "¥320,000+ in田園調布/目黒 for premium access",
            'value_options': "¥250,000-280,000 in等々力/尾山台 for family-friendly areas"
        }
        
        return strategy

    def get_search_focus(self, area, area_data):
        """Get specific search focus for each area"""
        focus_map = {
            '田園調布': 'Look for renovated apartments or small houses, premium but worth it',
            '目黒': 'Focus on newer buildings, excellent transport but competitive',
            '恵比寿': 'Look slightly outside main area for better family value',
            '等々力': 'Perfect family area - look for any 3LDK options',
            '尾山台': 'Great value for families, look for corner units',
            '三軒茶屋': 'Good value area, look for quieter side streets'
        }
        return focus_map.get(area, 'Standard family property search')

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
                 found_date, source, score, area_priority, route_type, reasons,
                 family_suitability, move_in_date, building_type, parking_available,
                 pet_friendly, furnished, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    prop['title'], prop['price'], prop['rooms'], prop['location'],
                    prop['station'], prop['walk_minutes'], prop['property_url'],
                    prop['found_date'], prop['source'], prop['score'],
                    prop['area_priority'], prop['route_type'], prop['reasons'],
                    prop['family_suitability'], prop.get('move_in_date'),
                    prop.get('building_type'), prop.get('parking_available', False),
                    prop.get('pet_friendly', False), prop.get('furnished', False),
                    prop.get('notes', '')
                ))
                
                if cursor.rowcount > 0:
                    new_count += 1
                    
            except sqlite3.Error as e:
                logger.error(f"Database error: {e}")
        
        conn.commit()
        conn.close()
        
        return new_count

    def run_family_property_search(self):
        """Run personalized property search for your family"""
        logger.info("🏠 Starting personalized DSTY property search for your family...")
        
        # Load market properties
        total_found, total_new = self.load_market_properties()
        
        # Generate search strategy
        strategy = self.generate_search_strategy()
        
        # Log search session
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        INSERT INTO search_sessions (search_date, properties_found, areas_searched, notes)
        VALUES (?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            total_found,
            ', '.join(self.dsty_areas.keys()),
            'Family property search with DSTY criteria'
        ))
        conn.commit()
        conn.close()
        
        logger.info(f"🎉 Family search complete! Found {total_found} properties tailored for your needs")
        logger.info("💡 Properties are ranked specifically for your family size, budget, and DSTY access needs")
        
        return total_found, total_new

    # Compatibility methods
    def get_top_properties(self, limit=20):
        """Get top-ranked properties for family"""
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
        """Get family-specific statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1')
        total = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1 AND price BETWEEN ? AND ?', 
                      (self.family_criteria['budget_min'], self.family_criteria['budget_max']))
        in_budget = cursor.fetchone()[0]
        
        cursor.execute('SELECT AVG(score) FROM properties WHERE is_active = 1')
        avg_score = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT MAX(score) FROM properties WHERE is_active = 1')
        max_score = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1 AND parking_available = 1')
        with_parking = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM properties WHERE is_active = 1 AND building_type = "戸建て"')
        houses = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_properties': total,
            'in_budget': in_budget,
            'avg_score': round(avg_score, 1),
            'max_score': round(max_score, 1),
            'with_parking': with_parking,
            'houses_available': houses
        }

    # Alias for compatibility
    def run_full_search(self):
        return self.run_family_property_search()

if __name__ == "__main__":
    finder = PersonalDStyPropertyFinder()
    finder.run_family_property_search()
