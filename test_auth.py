#!/usr/bin/env python3
"""
Test script specifically for Firebolt authentication credential persistence
"""

import os
import asyncio
from converter.firebolt_client import FireboltClient

async def test_credential_persistence():
    """Test that credentials persist through connection failures"""
    
    print("🔐 Testing Firebolt Credential Persistence...")
    
    # Test with your actual credentials
    client_id = "e0wMJJHiAnn4exI1GhwFlhE3VjUmIwD1"
    client_secret = "3kJRpMC65X-KU3ul_b8NUX7Th7y58lTFoyADp4vNZTmJWcCdc_SkJ_4n0tphrO07"
    account = "vishnu"
    database = "ugrodb"
    engine = "fb_clone_db_jauneet"
    
    # Create client
    client = FireboltClient()
    
    print(f"✅ Client created")
    
    # Connect
    success = await client.connect(
        client_id=client_id,
        client_secret=client_secret,
        account=account,
        database=database,
        engine=engine
    )
    
    if success:
        print(f"✅ Initial connection successful")
    else:
        print(f"❌ Initial connection failed")
        return False
    
    # Test that credentials are preserved
    print(f"🔍 Checking credential preservation...")
    print(f"   Client ID present: {bool(client.client_id)}")
    print(f"   Client Secret present: {bool(client.client_secret)}")
    print(f"   Account: {client.account}")
    print(f"   Database: {client.database}")
    print(f"   Engine: {client.engine}")
    
    # Simulate connection loss by setting connection to None
    print(f"🔧 Simulating connection loss...")
    client.connection = None
    
    # Try to execute a query (should trigger re-authentication)
    print(f"🧪 Testing query execution after connection loss...")
    success, result = await client.execute_query("SELECT 1 as test")
    
    if success:
        print(f"✅ Query executed successfully after re-authentication!")
        print(f"   Result: {result}")
        return True
    else:
        print(f"❌ Query failed after re-authentication")
        print(f"   Error: {result.get('error', 'Unknown error')}")
        return False

async def main():
    try:
        success = await test_credential_persistence()
        if success:
            print(f"\n🎉 Authentication persistence test PASSED!")
        else:
            print(f"\n❌ Authentication persistence test FAILED!")
        return success
    except Exception as e:
        print(f"\n💥 Test failed with exception: {str(e)}")
        return False

if __name__ == "__main__":
    result = asyncio.run(main())
    exit(0 if result else 1) 