#!/usr/bin/env python3
"""
Quick test script to verify all fixes work before deployment
"""

import os
import sys

# Import modules at top level
try:
    from converter.firebolt_client import FireboltClient
    from converter.query_converter import PostgreSQLToFireboltConverter
    from converter.live_tester import LiveQueryTester
    IMPORTS_OK = True
except Exception as e:
    print(f"❌ Import failed: {e}")
    IMPORTS_OK = False

def test_imports():
    """Test that all modules can be imported"""
    if IMPORTS_OK:
        print("✅ All imports successful")
        return True
    else:
        print("❌ Imports failed")
        return False

def test_firebolt_client():
    """Test FireboltClient initialization"""
    if not IMPORTS_OK:
        print("❌ Skipping test - imports failed")
        return False
        
    try:
        # Test empty initialization (should work now)
        client = FireboltClient()
        print("✅ FireboltClient can be initialized without parameters")
        
        # Test with parameters
        client2 = FireboltClient(
            client_id="test", 
            client_secret="test", 
            account="test", 
            database="test"
        )
        print("✅ FireboltClient can be initialized with parameters")
        return True
    except Exception as e:
        print(f"❌ FireboltClient test failed: {e}")
        return False

def test_converter():
    """Test converter initialization"""
    if not IMPORTS_OK:
        print("❌ Skipping test - imports failed")
        return False
        
    try:
        # Test without OpenAI key (should handle gracefully)
        converter = PostgreSQLToFireboltConverter()
        print("✅ Converter can be initialized without OpenAI key")
        
        # Test with OpenAI key
        if os.getenv("OPENAI_API_KEY"):
            converter2 = PostgreSQLToFireboltConverter(openai_api_key=os.getenv("OPENAI_API_KEY"))
            print("✅ Converter can be initialized with OpenAI key")
        else:
            print("⚠️ No OPENAI_API_KEY found for full test")
        
        return True
    except Exception as e:
        print(f"❌ Converter test failed: {e}")
        return False

def main():
    print("🧪 Testing PostgreSQL to Firebolt Converter Fixes...")
    print("=" * 60)
    
    tests = [
        ("Import Test", test_imports),
        ("FireboltClient Test", test_firebolt_client),
        ("Converter Test", test_converter),
    ]
    
    passed = 0
    for name, test_func in tests:
        print(f"\n🔬 Running {name}...")
        if test_func():
            passed += 1
        else:
            print(f"❌ {name} failed")
    
    print(f"\n📊 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("\n🎉 All tests passed! Ready for deployment.")
        print("\n🚀 To run Streamlit app locally:")
        print("   streamlit run streamlit_app.py")
        print("\n🔑 Make sure to set environment variables:")
        print("   export OPENAI_API_KEY='your_key_here'")
        return True
    else:
        print(f"\n❌ {len(tests) - passed} tests failed. Fix issues before deployment.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 