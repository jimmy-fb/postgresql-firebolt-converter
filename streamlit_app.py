import streamlit as st
import asyncio
import json
import os
from typing import Dict, Any
import logging
from dotenv import load_dotenv

# Load environment variables from .env by default
load_dotenv()

# Configure logging for Streamlit
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import our converter components
from converter.query_converter import PostgreSQLToFireboltConverter
from converter.firebolt_client import FireboltClient
from converter.live_tester import LiveQueryTester

# Page config
st.set_page_config(
    page_title="PostgreSQL to Firebolt Converter",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'firebolt_client' not in st.session_state:
    st.session_state.firebolt_client = None
if 'query_converter' not in st.session_state:
    st.session_state.query_converter = None
if 'live_tester' not in st.session_state:
    st.session_state.live_tester = None
if 'connection_status' not in st.session_state:
    st.session_state.connection_status = "Not Connected"
if 'ai_polish_enabled' not in st.session_state:
    st.session_state.ai_polish_enabled = False
if 'openai_key' not in st.session_state:
    # Default to .env OPENAI_API_KEY if present
    st.session_state.openai_key = os.getenv("OPENAI_API_KEY")
if 'ai_autocorrect_enabled' not in st.session_state:
    st.session_state.ai_autocorrect_enabled = False

def create_components():
    """Initialize the converter components"""
    # Try to get OpenAI API key from secrets first, then environment, then session state
    openai_api_key = None
    try:
        openai_api_key = st.secrets.get("OPENAI_API_KEY")
    except (FileNotFoundError, KeyError):
        openai_api_key = os.getenv("OPENAI_API_KEY") or st.session_state.openai_key
    
    try:
        # Initialize components
        st.session_state.query_converter = PostgreSQLToFireboltConverter(openai_api_key=openai_api_key)
        st.session_state.firebolt_client = FireboltClient()
        st.session_state.live_tester = LiveQueryTester(
            firebolt_client=st.session_state.firebolt_client,
            query_converter=st.session_state.query_converter,
            openai_api_key=openai_api_key
        )
        return True
    except Exception as e:
        st.error(f"Failed to initialize components: {str(e)}")
        return False

async def setup_firebolt_connection(client_id, client_secret, account, database, engine):
    """Setup Firebolt connection"""
    try:
        success = await st.session_state.firebolt_client.connect(
            client_id=client_id,
            client_secret=client_secret,
            account=account,
            database=database,
            engine=engine
        )
        return success, "Connected successfully!" if success else "Connection failed"
    except Exception as e:
        return False, f"Connection error: {str(e)}"

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🔄 PostgreSQL to Firebolt SQL Converter</h1>
        <p>AI-Powered Query Conversion with Live Testing & Auto-Correction</p>
    </div>
    """, unsafe_allow_html=True)

    # AI Settings section in sidebar
    st.sidebar.header("🤖 AI Settings (Optional)")
    with st.sidebar.expander("OpenAI Options", expanded=False):
        if os.getenv("OPENAI_API_KEY") and not st.session_state.openai_key:
            st.caption("Using OPENAI_API_KEY from .env by default")
        input_key = st.text_input("OpenAI API Key", value="", type="password", placeholder="Using .env OPENAI_API_KEY by default; enter here to overwrite")
        enable_ai_polish = st.checkbox("Enable OpenAI polish for conversion", value=st.session_state.ai_polish_enabled)
        enable_ai_autocorrect = st.checkbox("Enable OpenAI for auto-correction (live testing)", value=st.session_state.ai_autocorrect_enabled)
        if st.button("Save AI Settings"):
            st.session_state.openai_key = input_key.strip() or None
            st.session_state.ai_polish_enabled = bool(enable_ai_polish)
            st.session_state.ai_autocorrect_enabled = bool(enable_ai_autocorrect)
            # Propagate to environment for converter to read
            os.environ['ENABLE_AI_POLISH'] = 'true' if st.session_state.ai_polish_enabled else 'false'
            if st.session_state.openai_key:
                os.environ['OPENAI_API_KEY'] = st.session_state.openai_key
            # Recreate components so new key is used
            create_components()
            st.success("AI settings saved")

    # Initialize components (does not require OpenAI key)
    if not create_components():
        st.stop()

    # Sidebar for Firebolt connection
    st.sidebar.header("🔗 Firebolt Connection")
    
    # Try to get credentials from secrets first, then environment variables
    def get_secret_or_env(key):
        try:
            return st.secrets.get(key)
        except (FileNotFoundError, KeyError):
            return os.getenv(key, "")
    
    default_client_id = get_secret_or_env("FIREBOLT_CLIENT_ID")
    default_client_secret = get_secret_or_env("FIREBOLT_CLIENT_SECRET")
    default_account = get_secret_or_env("FIREBOLT_ACCOUNT")
    default_database = get_secret_or_env("FIREBOLT_DATABASE")
    default_engine = get_secret_or_env("FIREBOLT_ENGINE")
    
    # Show connection status
    st.sidebar.write(f"**Status:** {st.session_state.connection_status}")
    
    # If we have all secrets, auto-connect
    if all([default_client_id, default_client_secret, default_account, default_database, default_engine]) and st.session_state.connection_status == "Not Connected":
        with st.sidebar:
            st.info("🔐 Using credentials from secrets...")
            with st.spinner("Auto-connecting to Firebolt..."):
                success, message = asyncio.run(setup_firebolt_connection(
                    default_client_id, default_client_secret, default_account, default_database, default_engine
                ))
                
                if success:
                    st.session_state.connection_status = "Connected ✅"
                    st.success("Auto-connected successfully!")
                else:
                    st.session_state.connection_status = "Failed ❌"
                    st.error(f"Auto-connection failed: {message}")
    
    # Manual connection form (fallback or override)
    with st.sidebar.expander("🔧 Manual Connection (Optional)", expanded=not bool(default_client_id)):
        with st.form("connection_form"):
            # Show masked values if using secrets, empty if manual entry
            display_client_id = "••••••••••••••••" if default_client_id else ""
            display_client_secret = "••••••••••••••••" if default_client_secret else ""
            display_account = "••••••••" if default_account else ""
            display_database = "••••••••" if default_database else ""
            display_engine = "••••••••" if default_engine else ""
            
            client_id = st.text_input("Client ID", 
                                     value=display_client_id if default_client_id else "", 
                                     type="password",
                                     placeholder="Will use secret if available" if default_client_id else "Enter your Firebolt Client ID")
            client_secret = st.text_input("Client Secret", 
                                         value=display_client_secret if default_client_secret else "", 
                                         type="password",
                                         placeholder="Will use secret if available" if default_client_secret else "Enter your Firebolt Client Secret")
            account = st.text_input("Account", 
                                   value=display_account if default_account else "",
                                   placeholder="Will use secret if available" if default_account else "Enter your Firebolt Account")
            database = st.text_input("Database", 
                                    value=display_database if default_database else "",
                                    placeholder="Will use secret if available" if default_database else "Enter your Firebolt Database")
            engine = st.text_input("Engine", 
                                  value=display_engine if default_engine else "",
                                  placeholder="Will use secret if available" if default_engine else "Enter your Firebolt Engine")
            
            connect_btn = st.form_submit_button("Connect to Firebolt")
            
            if connect_btn:
                # Use actual secret values if form shows masked values, otherwise use form input
                actual_client_id = default_client_id if (client_id == display_client_id and default_client_id) else client_id
                actual_client_secret = default_client_secret if (client_secret == display_client_secret and default_client_secret) else client_secret
                actual_account = default_account if (account == display_account and default_account) else account
                actual_database = default_database if (database == display_database and default_database) else database
                actual_engine = default_engine if (engine == display_engine and default_engine) else engine
                
                if all([actual_client_id, actual_client_secret, actual_account, actual_database, actual_engine]):
                    with st.spinner("Connecting to Firebolt..."):
                        success, message = asyncio.run(setup_firebolt_connection(
                            actual_client_id, actual_client_secret, actual_account, actual_database, actual_engine
                        ))
                        
                        if success:
                            st.session_state.connection_status = "Connected ✅"
                            st.success(message)
                        else:
                            st.session_state.connection_status = "Failed ❌"
                            st.error(message)
                else:
                    st.error("Please provide all connection details")

    # Main content area
    col1, col2 = st.columns([1, 1])

    with col1:
        st.header("📝 PostgreSQL Query")
        postgresql_query = st.text_area(
            "Enter your PostgreSQL query:",
            height=300,
            value="""select sa.applicationid as LAF,
sum(saf.totalfeeamount::decimal) filter (where sfm.feedesc = 'Login Fees')::text as IMD,
sum(saf.totalfeeamount::decimal) filter (where sfm.feedesc = 'Insurance Penetration Percentage')::text as Total_Insurance,
sum(saf.totalfeeamount::decimal) filter (where sfm.feedesc = 'Processing Fees')::text as Processing_Fees,
'skaleup' as source,
now() at TIME ZONE 'Asia/Kolkata' as edl_job_run
from skaleup_application sa
left join skaleup_application_fee saf on sa.applicationkey = saf.applicationkey and saf.isactive = TRUE
left join skaleup_fee_master sfm on saf.feetypecode = sfm.feecode and sfm.isactive = true
where sa.isactive = true
group by sa.applicationid
order by sa.applicationid"""
        )

    with col2:
        st.header("🎯 Firebolt Query")
        
        # Determine what query to show
        display_query = ""
        if 'final_working_query' in st.session_state:
            display_query = st.session_state.final_working_query
            st.info("✅ This is the **tested and working** Firebolt query from live testing!")
        
        # Convert button
        if st.button("🔄 Convert Query", type="primary"):
            if postgresql_query.strip():
                with st.spinner("Converting query..."):
                    try:
                        # Ensure env flag is set from session state for this run
                        os.environ['ENABLE_AI_POLISH'] = 'true' if st.session_state.ai_polish_enabled else 'false'
                        result = st.session_state.query_converter.convert(postgresql_query)
                        converted_sql = result.get('converted_sql', 'Conversion failed')
                        
                        # Update the display query
                        display_query = converted_sql
                        
                        # Clear any previous final working query since this is a new conversion
                        if 'final_working_query' in st.session_state:
                            del st.session_state.final_working_query
                        
                        # Show warnings/explanations if available
                        warnings = result.get('warnings', [])
                        explanations = result.get('explanations', [])
                        method_used = result.get('method_used', 'unknown')
                        if method_used:
                            st.caption(f"Method used: {method_used}")
                        if warnings:
                            with st.expander("⚠️ Warnings", expanded=False):
                                for w in warnings:
                                    st.warning(w)
                        if explanations:
                            with st.expander("✅ Conversions Applied", expanded=False):
                                for ex in explanations:
                                    st.write(f"- {ex}")
                    except Exception as e:
                        st.error(f"Conversion failed: {str(e)}")
                        display_query = f"Conversion failed: {str(e)}"
            else:
                st.warning("Please enter a PostgreSQL query to convert.")
        
        # Display the query in a text area
        st.text_area(
            "Converted/Working Firebolt Query:",
            value=display_query,
            height=300,
            key="firebolt_query_display",
            help="This shows either the converted query or the final working query after live testing"
        )
        
        # Add copy button and additional info for working queries
        if 'final_working_query' in st.session_state:
            col_copy, col_info = st.columns([1, 2])
            
            with col_copy:
                if st.button("📋 Copy Working Query", type="secondary"):
                    st.code(st.session_state.final_working_query, language='sql')
                    
            with col_info:
                if 'test_result' in st.session_state:
                    result = st.session_state.test_result
                    total_attempts = result.get('total_attempts', 'Unknown')
                    st.caption(f"✅ Tested successfully in {total_attempts} attempt(s)")
                    
                    # Show row count if available
                    if result.get('final_result') and 'row_count' in result.get('final_result', {}):
                        row_count = result['final_result']['row_count']
                        st.caption(f"📊 Query returned {row_count:,} rows")
        
        elif display_query and display_query != "":
            st.caption("💡 Use 'Test & Auto-Fix Query' below to verify this query works on your Firebolt database")

    # Live Testing Section
    st.header("🧪 Live Testing & Auto-Correction")
    
    if st.session_state.connection_status == "Connected ✅":
        test_col1, test_col2 = st.columns([1, 1])
        
        with test_col1:
            if st.button("🚀 Test & Auto-Fix Query", type="secondary"):
                if postgresql_query.strip():
                    with st.spinner("Testing query and auto-correcting errors..."):
                        try:
                            # Check if credentials are available and auto-fix if needed
                            client = st.session_state.live_tester.firebolt_client
                            
                            if not all([client.client_id, client.client_secret, client.account, client.database]):
                                st.warning("🔧 Credentials missing, attempting to restore...")
                                
                                def get_secret_or_env(key):
                                    try:
                                        return st.secrets.get(key)
                                    except (FileNotFoundError, KeyError):
                                        return os.getenv(key, "")
                                
                                fix_client_id = get_secret_or_env("FIREBOLT_CLIENT_ID")
                                fix_client_secret = get_secret_or_env("FIREBOLT_CLIENT_SECRET")
                                fix_account = get_secret_or_env("FIREBOLT_ACCOUNT")
                                fix_database = get_secret_or_env("FIREBOLT_DATABASE")
                                fix_engine = get_secret_or_env("FIREBOLT_ENGINE")
                                
                                if all([fix_client_id, fix_client_secret, fix_account, fix_database]):
                                    success = asyncio.run(client.connect(
                                        client_id=fix_client_id,
                                        client_secret=fix_client_secret,
                                        account=fix_account,
                                        database=fix_database,
                                        engine=fix_engine
                                    ))
                                    if not success:
                                        st.error("❌ Could not reconnect with restored credentials.")
                                        st.stop()
                                else:
                                    st.error("❌ No backup credentials found in secrets or environment.")
                                    st.stop()
                            
                            # Update OpenAI key for live tester if user provided one
                            if st.session_state.ai_autocorrect_enabled and st.session_state.openai_key:
                                os.environ['OPENAI_API_KEY'] = st.session_state.openai_key
                                # Recreate with latest key
                                st.session_state.live_tester = LiveQueryTester(
                                    firebolt_client=st.session_state.firebolt_client,
                                    query_converter=st.session_state.query_converter,
                                    openai_api_key=st.session_state.openai_key
                                )
                            elif not st.session_state.ai_autocorrect_enabled:
                                # Create tester without OpenAI
                                st.session_state.live_tester = LiveQueryTester(
                                    firebolt_client=st.session_state.firebolt_client,
                                    query_converter=st.session_state.query_converter,
                                    openai_api_key=None
                                )

                            # Run the live testing
                            result = asyncio.run(
                                st.session_state.live_tester.test_and_fix_query(
                                    postgresql_query, 
                                    max_correction_attempts=15
                                )
                            )
                            
                            st.session_state.test_result = result
                            
                            # If testing was successful, update the Firebolt query display
                            if result.get('success') and result.get('final_query'):
                                st.session_state.final_working_query = result.get('final_query')
                                st.success("✅ Query tested successfully! The working Firebolt query has been updated below.")
                                st.rerun()  # Refresh to show the updated query
                            
                        except Exception as e:
                            st.error(f"Testing failed: {str(e)}")
                            import traceback
                            st.code(traceback.format_exc())
                else:
                    st.warning("Please enter a PostgreSQL query to test.")
        
        # Display test results
        if 'test_result' in st.session_state:
            result = st.session_state.test_result
            
            with test_col2:
                if result.get('success'):
                    st.markdown('<div class="success-box">✅ <strong>Query Successful!</strong></div>', unsafe_allow_html=True)
                    st.code(result.get('final_query', ''), language='sql')
                else:
                    st.markdown('<div class="error-box">❌ <strong>Query Failed</strong></div>', unsafe_allow_html=True)
            
            # Attempt details
            st.subheader("📊 Attempt Details")
            attempts = result.get('attempts', [])
            
            for attempt in attempts:
                attempt_num = attempt.get('attempt', 0)
                success = attempt.get('success', False)
                
                with st.expander(f"Attempt {attempt_num} {'✅' if success else '❌'}"):
                    if success:
                        st.success("Query executed successfully!")
                        if 'result' in attempt:
                            st.write("Query result preview:", attempt['result'])
                    else:
                        st.error(f"Error: {attempt.get('error', 'Unknown error')}")
                    
                    st.code(attempt.get('converted_query', ''), language='sql')
    else:
        st.warning("⚠️ Please connect to Firebolt first to enable live testing.")

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666;">
        <p>🚀 Built with Streamlit | 🤖 Powered by OpenAI | 🔥 Firebolt Integration</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 