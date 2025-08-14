import os
import json
import asyncio
import logging
import traceback
from flask import Flask, request, jsonify
from converter.query_converter import PostgreSQLToFireboltConverter
from converter.firebolt_client import FireboltClient
from converter.live_tester import LiveQueryTester
from converter.mcp_client import FireboltMCPClient
import difflib

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("🔄 Starting PostgreSQL to Firebolt Converter...")
print("🌐 Server will be available at: http://0.0.0.0:8080")
print("🔑 Make sure to set OPENAI_API_KEY in .env for AI-powered conversions")

# Global instances
firebolt_client = None
query_converter = None
live_tester = None
mcp_client = None

# Connection storage
saved_connections = {}

def safe_json_serialize(obj):
    """Safely serialize objects to JSON, handling dict_keys and other non-serializable types"""
    if isinstance(obj, dict):
        return {k: safe_json_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [safe_json_serialize(item) for item in obj]
    elif hasattr(obj, 'keys') and callable(obj.keys):  # dict_keys, dict_values, etc.
        return list(obj.keys()) if hasattr(obj, 'keys') else list(obj)
    elif hasattr(obj, '__dict__'):
        return safe_json_serialize(obj.__dict__)
    else:
        try:
            json.dumps(obj)  # Test if it's JSON serializable
            return obj
        except (TypeError, ValueError):
            return str(obj)  # Convert to string as fallback

@app.route('/')
def index():
    """Serve the main HTML interface"""
    with open('templates/index.html', 'r') as f:
        template_content = f.read()
    return template_content

@app.route('/convert', methods=['POST'])
def convert_query():
    """Convert PostgreSQL query to Firebolt"""
    global query_converter
    
    try:
        if not query_converter:
            query_converter = PostgreSQLToFireboltConverter()
            
        data = request.get_json()
        postgresql_query = data.get('postgresql_query', '').strip()
        
        if not postgresql_query:
            return jsonify({'error': 'No PostgreSQL query provided'}), 400
        
        # Convert the query
        result = query_converter.convert(postgresql_query)
        
        return jsonify({
            'converted_sql': result.get('converted_sql', ''),
            'method_used': result.get('method_used', 'rule_based'),
            'warnings': result.get('warnings', []),
            'explanations': result.get('explanations', [])
        })
        
    except Exception as e:
        logger.error(f"Conversion error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/setup-firebolt', methods=['POST'])
async def setup_firebolt_connection():
    """Setup Firebolt connection with credentials"""
    try:
        data = request.get_json()
        required_fields = ['client_id', 'client_secret', 'account', 'database']
        
        logger.info(f"Received Firebolt setup request: {list(data.keys())}")
        
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return jsonify({
                'error': f'Missing required fields: {", ".join(missing_fields)}'
            }), 400
        
        client_id = data['client_id']
        client_secret = data['client_secret']
        account = data['account']
        database = data['database']
        engine = data.get('engine')  # Optional - can be None for system engine
        
        logger.info(f"Setup details - Account: {account}, Database: {database}, Engine: {engine}")
        
        result = await setup_firebolt_connection_internal(client_id, client_secret, account, database, engine)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400 if 'error' not in result else 500
            
    except Exception as e:
        logger.error(f"Firebolt setup error: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-and-fix', methods=['POST'])
async def test_and_fix():
    """Test and fix a PostgreSQL query against live Firebolt"""
    global live_tester
    
    try:
        data = request.get_json()
        postgresql_query = data.get('postgresql_query', '').strip()
        
        if not postgresql_query:
            return jsonify({'error': 'No PostgreSQL query provided'}), 400
            
        if not live_tester:
            return jsonify({'error': 'Firebolt connection not established. Please setup connection first.'}), 400
        
        logger.info(f"Testing and fixing query: {postgresql_query[:100]}...")
        
        # Run the test and fix cycle
        result = await live_tester.test_and_fix_query(postgresql_query, max_correction_attempts=15)
        
        # Use safe JSON serialization
        safe_result = safe_json_serialize(result)
        
        return jsonify({
            'success': True,
            'result': safe_result
        })
        
    except Exception as e:
        logger.error(f"Live testing error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/save-connection', methods=['POST'])
def save_connection():
    """Save current connection with a name"""
    global firebolt_client, saved_connections
    
    try:
        data = request.get_json()
        connection_name = data.get('name', '').strip()
        
        if not connection_name:
            return jsonify({'error': 'Connection name is required'}), 400
            
        if not firebolt_client:
            return jsonify({'error': 'No active connection to save'}), 400
        
        # Save connection info
        connection_info = firebolt_client.get_connection_info()
        saved_connections[connection_name] = {
            'client_id': connection_info.get('client_id'),
            'client_secret': connection_info.get('client_secret'),  
            'account': connection_info.get('account'),
            'database': connection_info.get('database'),
            'engine': connection_info.get('engine'),
            'saved_at': connection_info.get('connected_at')
        }
        
        return jsonify({
            'success': True,
            'message': f'Connection "{connection_name}" saved successfully',
            'saved_connections': list(saved_connections.keys())
        })
        
    except Exception as e:
        logger.error(f"Save connection error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/load-connection', methods=['POST'])
async def load_connection():
    """Load a saved connection"""
    global firebolt_client, query_converter, live_tester, mcp_client, saved_connections
    
    try:
        data = request.get_json()
        connection_name = data.get('name', '').strip()
        
        if not connection_name:
            return jsonify({'error': 'Connection name is required'}), 400
            
        if connection_name not in saved_connections:
            return jsonify({'error': f'Connection "{connection_name}" not found'}), 404
        
        conn_data = saved_connections[connection_name]
        
        # Setup connection using saved data
        result = await setup_firebolt_connection_internal(
            conn_data['client_id'],
            conn_data['client_secret'],
            conn_data['account'],
            conn_data['database'],
            conn_data['engine']
        )
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': f'Connection "{connection_name}" loaded successfully',
                'connection_info': result['connection_info']
            })
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"Load connection error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/list-connections', methods=['GET'])
def list_connections():
    """List all saved connections"""
    return jsonify({
        'success': True,
        'connections': list(saved_connections.keys())
    })

async def setup_firebolt_connection_internal(client_id, client_secret, account, database, engine):
    """Internal function to setup Firebolt connection"""
    global firebolt_client, query_converter, live_tester, mcp_client
    
    try:
        # Initialize MCP client
        mcp_client = FireboltMCPClient(client_id, client_secret)
        await mcp_client.connect()
        
        # Initialize Firebolt client
        firebolt_client = FireboltClient(client_id, client_secret, account, database, engine)
        
        # Test connection
        success, message = await firebolt_client.test_connection()
        
        if not success:
            return {
                'success': False,
                'message': message
            }
        
        logger.info(f"Firebolt connection established for account: {account}")
        
        # Initialize converter and live tester with proper parameters
        query_converter = PostgreSQLToFireboltConverter(
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            use_mcp=True,
            mcp_client=mcp_client
        )
        live_tester = LiveQueryTester(firebolt_client, query_converter, mcp_client)
        
        return {
            'success': True,
            'message': 'Connection successful! Firebolt is ready for query conversion.',
            'connection_info': firebolt_client.get_connection_info()
        }
        
    except Exception as e:
        logger.error(f"Firebolt setup error: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

@app.route('/api/firebolt-status', methods=['GET'])
def get_firebolt_status():
    """Get current Firebolt connection status"""
    global live_tester
    
    if not live_tester:
        return jsonify({
            'connected': False,
            'message': 'No Firebolt connection configured'
        })
    
    try:
        # Test connection
        async def test_status():
            return await live_tester.test_connection()
        
        success, message = asyncio.run(test_status())
        
        return jsonify({
            'connected': success,
            'message': message,
            'connection_info': live_tester.firebolt_client.get_connection_info()
        })
        
    except Exception as e:
        return jsonify({
            'connected': False,
            'message': f'Connection test failed: {str(e)}'
        })

@app.route('/api/compare', methods=['POST'])
def compare_queries():
    """Compare converter output with manual conversion for accuracy assessment"""
    try:
        data = request.get_json()
        postgresql_query = data.get('postgresql_query', '')
        manual_firebolt = data.get('manual_firebolt', '')
        
        if not postgresql_query or not manual_firebolt:
            return jsonify({'error': 'Both PostgreSQL query and manual Firebolt conversion are required'}), 400
        
        # Convert using our tool
        result = query_converter.convert(postgresql_query)
        converter_output = result.get('converted_sql', '')
        
        # Compare queries
        exact_match = converter_output.strip().lower() == manual_firebolt.strip().lower()
        
        # Calculate similarity score
        similarity = difflib.SequenceMatcher(None, 
                                           converter_output.strip().lower(), 
                                           manual_firebolt.strip().lower()).ratio()
        similarity_score = round(similarity * 100, 1)
        
        # Generate diff if not exact match
        diff = None
        if not exact_match:
            diff_lines = list(difflib.unified_diff(
                converter_output.splitlines(keepends=True),
                manual_firebolt.splitlines(keepends=True),
                fromfile='Converter Output',
                tofile='Manual Conversion',
                lineterm=''
            ))
            diff = ''.join(diff_lines) if diff_lines else None
        
        return jsonify({
            'success': True,
            'exact_match': exact_match,
            'similarity_score': similarity_score,
            'converter_output': converter_output,
            'manual_conversion': manual_firebolt,
            'diff': diff,
            'warnings': result.get('warnings', []),
            'explanation': result.get('explanation', [])
        })
        
    except Exception as e:
        logger.error(f"Comparison error: {str(e)}")
        return jsonify({'error': f'Comparison failed: {str(e)}'}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=True) 