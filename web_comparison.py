#!/usr/bin/env python3
"""
Web-based comparison interface for testing converter accuracy
"""

from flask import Flask, render_template, request, jsonify
import os
from dotenv import load_dotenv
from converter.query_converter import PostgreSQLToFireboltConverter
import difflib

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'comparison-test-key')

# Initialize converter
converter = PostgreSQLToFireboltConverter()

def normalize_sql(query: str) -> str:
    """Normalize SQL for comparison"""
    import re
    query = re.sub(r'\s+', ' ', query.strip())
    query = query.rstrip(';')
    return query

@app.route('/compare')
def comparison_page():
    """Comparison test page"""
    return render_template('comparison.html')

@app.route('/api/compare', methods=['POST'])
def compare_queries():
    """API endpoint to compare PostgreSQL query conversion with expected result"""
    try:
        data = request.get_json()
        postgresql_query = data.get('postgresql_query', '').strip()
        expected_firebolt = data.get('expected_firebolt', '').strip()
        
        if not postgresql_query:
            return jsonify({'error': 'PostgreSQL query is required'}), 400
            
        if not expected_firebolt:
            return jsonify({'error': 'Expected Firebolt query is required'}), 400
        
        # Convert the query
        conversion_result = converter.convert(postgresql_query)
        converted_query = conversion_result['converted_query']
        
        # Normalize for comparison
        converted_norm = normalize_sql(converted_query)
        expected_norm = normalize_sql(expected_firebolt)
        
        exact_match = converted_norm == expected_norm
        
        # Generate diff if not exact match
        diff_html = ""
        if not exact_match:
            diff = list(difflib.unified_diff(
                expected_norm.splitlines(keepends=True),
                converted_norm.splitlines(keepends=True),
                fromfile='Expected Firebolt',
                tofile='Converter Output',
                lineterm=''
            ))
            diff_html = '\n'.join(diff)
        
        # Calculate similarity score (simple approach)
        similarity = difflib.SequenceMatcher(None, expected_norm, converted_norm).ratio() * 100
        
        return jsonify({
            'success': True,
            'postgresql_query': postgresql_query,
            'expected_firebolt': expected_firebolt,
            'converted_query': converted_query,
            'exact_match': exact_match,
            'similarity_score': round(similarity, 1),
            'diff': diff_html,
            'normalized_expected': expected_norm,
            'normalized_converted': converted_norm,
            'warnings': conversion_result.get('warnings', []),
            'explanations': conversion_result.get('explanations', [])
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 3001))
    app.run(debug=True, host='0.0.0.0', port=port) 