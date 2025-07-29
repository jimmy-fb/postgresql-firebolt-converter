#!/usr/bin/env python3
"""
PostgreSQL to Firebolt Query Converter
Simple runner script
"""

import os
import sys
import asyncio

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from app import app
    
    print("🔄 Starting PostgreSQL to Firebolt Converter...")
    print("🌐 Server will be available at: http://0.0.0.0:8080")
    print("🔑 Make sure to set OPENAI_API_KEY in .env for AI-powered conversions")
    
    if __name__ == '__main__':
        # Get port from environment or default to 8080
        port = int(os.environ.get('PORT', 8080))
        
        # Run the Flask app
        app.run(
            host='0.0.0.0',
            port=port,
            debug=True,
            threaded=True
        )
        
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all dependencies are installed:")
    print("pip install flask firebolt-sdk openai")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error starting server: {e}")
    sys.exit(1) 