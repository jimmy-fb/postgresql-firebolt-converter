import streamlit as st
import asyncio
import json
import os
from typing import Dict, Any
import logging

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

def create_components():
    """Initialize the converter components"""
    openai_api_key = st.secrets.get("OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY")
    
    if not openai_api_key:
        st.error("⚠️ OpenAI API key not found. Please add it to Streamlit secrets or environment variables.")
        return False
    
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

    # Initialize components
    if not create_components():
        st.stop()

    # Sidebar for Firebolt connection
    st.sidebar.header("🔗 Firebolt Connection")
    
    with st.sidebar.form("connection_form"):
        client_id = st.text_input("Client ID", type="password")
        client_secret = st.text_input("Client Secret", type="password")
        account = st.text_input("Account")
        database = st.text_input("Database")
        engine = st.text_input("Engine")
        
        connect_btn = st.form_submit_button("Connect to Firebolt")
        
        if connect_btn and all([client_id, client_secret, account, database, engine]):
            with st.spinner("Connecting to Firebolt..."):
                success, message = asyncio.run(setup_firebolt_connection(
                    client_id, client_secret, account, database, engine
                ))
                
                if success:
                    st.session_state.connection_status = "Connected ✅"
                    st.success(message)
                else:
                    st.session_state.connection_status = "Failed ❌"
                    st.error(message)
    
    st.sidebar.write(f"**Status:** {st.session_state.connection_status}")

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
        
        # Convert button
        if st.button("🔄 Convert Query", type="primary"):
            if postgresql_query.strip():
                with st.spinner("Converting query..."):
                    try:
                        result = st.session_state.query_converter.convert(postgresql_query)
                        converted_sql = result.get('converted_sql', 'Conversion failed')
                        
                        st.text_area(
                            "Converted Firebolt Query:",
                            value=converted_sql,
                            height=300,
                            key="converted_query"
                        )
                        
                        # Store in session state for testing
                        st.session_state.last_converted = converted_sql
                        
                    except Exception as e:
                        st.error(f"Conversion failed: {str(e)}")
            else:
                st.warning("Please enter a PostgreSQL query to convert.")

    # Live Testing Section
    st.header("🧪 Live Testing & Auto-Correction")
    
    if st.session_state.connection_status == "Connected ✅":
        test_col1, test_col2 = st.columns([1, 1])
        
        with test_col1:
            if st.button("🚀 Test & Auto-Fix Query", type="secondary"):
                if postgresql_query.strip():
                    with st.spinner("Testing query and auto-correcting errors..."):
                        try:
                            # Run the live testing
                            result = asyncio.run(
                                st.session_state.live_tester.test_and_fix_query(
                                    postgresql_query, 
                                    max_correction_attempts=15
                                )
                            )
                            
                            st.session_state.test_result = result
                            
                        except Exception as e:
                            st.error(f"Testing failed: {str(e)}")
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