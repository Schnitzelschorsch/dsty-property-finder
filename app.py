# app.py - Web interface for Railway
from flask import Flask, render_template_string, jsonify
from crawler import DStyPropertyCrawler
import threading
import time
import schedule

app = Flask(__name__)
crawler = DStyPropertyCrawler()

# HTML template for the dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>DSTY Property Finder</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #3498db, #2c3e50); color: white; padding: 30px; border-radius: 10px; text-align: center; margin-bottom: 30px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: white; padding: 20px; border-radius: 10px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .stat-number { font-size: 2em; font-weight: bold; color: #3498db; }
        .stat-label { color: #666; margin-top: 5px; }
        .controls { text-align: center; margin: 30px 0; }
        .btn { background: #3498db; color: white; border: none; padding: 15px 30px; border-radius: 25px; cursor: pointer; font-size: 16px; margin: 10px; }
        .btn:hover { background: #2980b9; }
        .btn:disabled { opacity: 0.6; cursor: not-allowed; }
        .properties { display: grid; gap: 20px; }
        .property { background: white; padding: 25px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); border-left: 5px solid #3498db; }
        .property.rank-1 { border-left-color: #f1c40f; }
        .property.rank-2 { border-left-color: #95a5a6; }
        .property.rank-3 { border-left-color: #cd7f32; }
        .rank-badge { float: right; background: #3498db; color: white; border-radius: 50%; width: 40px; height: 40px; display: flex; align-items: center; justify-content: center; font-weight: bold; }
        .rank-1 .rank-badge { background: #f1c40f; }
        .rank-2 .rank-badge { background: #95a5a6; }
        .rank-3 .rank-badge { background: #cd7f32; }
        .score { color: #27ae60; font-weight: bold; font-size: 1.3em; margin-bottom: 10px; }
        .property-title { font-size: 1.2em; margin-bottom: 15px; color: #2c3e50; }
        .property-details { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; margin-bottom: 15px; }
        .detail-item { color: #555; }
        .reasons { background: #e8f5e8; padding: 15px; border-radius: 8px; margin-top: 15px; }
        .reasons h4 { color: #27ae60; margin-bottom: 10px; }
        .reasons ul { margin: 0; padding-left: 20px; }
        .loading { text-align: center; padding: 40px; color: #666; }
        .live-indicator { display: inline-block; width: 12px; height: 12px; background: #2ecc71; border-radius: 50%; margin-right: 8px; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .last-updated { color: #666; font-size: 0.9em; margin-top: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1><span class="live-indicator"></span>DSTY Property Finder</h1>
            <p>Live property search for Deutsche Schule Tokyo Yokohama families</p>
            <div class="last-updated" id="last-updated"></div>
        </div>
        
        <div class="stats" id="stats">
            <div class="stat-card">
                <div class="stat-number" id="total-properties">-</div>
                <div class="stat-label">Total Properties</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="in-budget">-</div>
                <div class="stat-label">In Your Budget</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="avg-score">-</div>
                <div class="stat-label">Average Score</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="max-score">-</div>
                <div class="stat-label">Highest Score</div>
            </div>
        </div>
        
        <div class="controls">
            <button class="btn" onclick="runSearch()" id="search-btn">🔍 Search Now</button>
            <button class="btn" onclick="refreshData()">🔄 Refresh Data</button>
            <button class="btn" onclick="exportCSV()">📊 Export CSV</button>
        </div>
        
        <div id="properties-container">
            <div class="loading">Loading properties...</div>
        </div>
    </div>
    
    <script>
        function loadData() {
            // Load statistics
            fetch('/api/stats')
                .then(response => response.json())
                .then(data => {
                    document.getElementById('total-properties').textContent = data.total_properties;
                    document.getElementById('in-budget').textContent = data.in_budget;
                    document.getElementById('avg-score').textContent = data.avg_score;
                    document.getElementById('max-score').textContent = data.max_score;
                });
            
            // Load properties
            fetch('/api/properties')
                .then(response => response.json())
                .then(properties => {
                    displayProperties(properties);
                });
            
            // Update last updated time
            document.getElementById('last-updated').textContent = 'Last updated: ' + new Date().toLocaleString();
        }
        
        function displayProperties(properties) {
            const container = document.getElementById('properties-container');
            
            if (properties.length === 0) {
                container.innerHTML = '<div class="loading">No properties found yet. Click "Search Now" to start finding properties!</div>';
                return;
            }
            
            const html = properties.map((prop, index) => `
                <div class="property rank-${Math.min(index + 1, 3)}">
                    <div class="rank-badge">#${index + 1}</div>
                    <div class="score">Score: ${prop.score}/100</div>
                    <div class="property-title">${prop.title}</div>
                    
                    <div class="property-details">
                        <div class="detail-item"><strong>💰 Price:</strong> ¥${prop.price.toLocaleString()}/month</div>
                        <div class="detail-item"><strong>🛏️ Rooms:</strong> ${prop.rooms}</div>
                        <div class="detail-item"><strong>📍 Location:</strong> ${prop.location}</div>
                        <div class="detail-item"><strong>🚇 Station:</strong> ${prop.station} (${prop.walk_minutes}min)</div>
                        <div class="detail-item"><strong>🚌 Route:</strong> ${prop.route_type} Route</div>
                        <div class="detail-item"><strong>🔗 Link:</strong> <a href="${prop.property_url}" target="_blank">View Property</a></div>
                    </div>
                    
                    <div class="reasons">
                        <h4>✨ Why this property scored well:</h4>
                        <ul>
                            ${prop.reasons.map(reason => `<li>${reason}</li>`).join('')}
                        </ul>
                    </div>
                </div>
            `).join('');
            
            container.innerHTML = '<div class="properties">' + html + '</div>';
        }
        
        function runSearch() {
            const btn = document.getElementById('search-btn');
            btn.disabled = true;
            btn.textContent = '🔍 Searching...';
            
            fetch('/api/search', {method: 'POST'})
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        alert(`Search complete! Found ${data.total_found} properties, ${data.total_new} new ones.`);
                        loadData();
                    } else {
                        alert('Search failed: ' + data.error);
                    }
                })
                .catch(error => {
                    alert('Search failed: ' + error);
                })
                .finally(() => {
                    btn.disabled = false;
                    btn.textContent = '🔍 Search Now';
                });
        }
        
        function refreshData() {
            loadData();
        }
        
        function exportCSV() {
            window.open('/api/export/csv');
        }
        
        // Load data on page load
        loadData();
        
        // Auto-refresh every 5 minutes
        setInterval(loadData, 5 * 60 * 1000);
    </script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    """Main dashboard"""
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/stats')
def api_stats():
    """Get statistics"""
    stats = crawler.get_stats()
    return jsonify(stats)

@app.route('/api/properties')
def api_properties():
    """Get properties"""
    properties = crawler.get_top_properties(50)
    return jsonify(properties)

@app.route('/api/search', methods=['POST'])
def api_search():
    """Trigger search"""
    try:
        total_found, total_new = crawler.run_full_search()
        return jsonify({
            'success': True,
            'total_found': total_found,
            'total_new': total_new
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/export/csv')
def export_csv():
    """Export properties as CSV"""
    from flask import Response
    import csv
    import io
    
    properties = crawler.get_top_properties(100)
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow(['Rank', 'Score', 'Title', 'Price', 'Rooms', 'Location', 'Station', 'Walk Time', 'Route', 'URL'])
    
    # Write data
    for i, prop in enumerate(properties, 1):
        writer.writerow([
            i, prop['score'], prop['title'], prop['price'], prop['rooms'],
            prop['location'], prop['station'], prop['walk_minutes'],
            prop['route_type'], prop['property_url']
        ])
    
    output.seek(0)
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment;filename=dsty_properties.csv'}
    )

def run_scheduled_search():
    """Background scheduler for automatic searches"""
    schedule.every(4).hours.do(lambda: crawler.run_full_search())
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == '__main__':
    # Start background scheduler
    scheduler_thread = threading.Thread(target=run_scheduled_search, daemon=True)
    scheduler_thread.start()
    
    # Run initial search
    crawler.run_full_search()
    
    # Start web server
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
