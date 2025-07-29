# 🔄 PostgreSQL to Firebolt SQL Converter

An **intelligent AI-powered tool** that converts PostgreSQL queries to Firebolt SQL with **live testing** and **auto-correction**. 

## ✨ Features

- 🤖 **AI-Powered Conversion**: Uses OpenAI ChatGPT for intelligent query conversion
- 🔥 **Live Firebolt Testing**: Tests converted queries against your real Firebolt database
- 🔄 **Smart Auto-Correction**: Intelligently fixes errors with context-aware retry logic
- 🧠 **Error Learning**: Tracks repeated errors and tries different approaches
- 💾 **Connection Management**: Save and reuse Firebolt connection credentials
- 🌐 **Web Interface**: Both Flask and Streamlit versions available
- 📊 **Detailed Attempt Tracking**: See exactly how queries are being fixed

## 🚀 Live Demo

**Streamlit Cloud:** [Coming Soon - Will be deployed after GitHub setup]

## 🏃‍♂️ Quick Start

### Option 1: Run Locally (Flask)

1. **Clone the repository:**
```bash
git clone https://github.com/YOUR_USERNAME/postgresql-firebolt-converter.git
cd postgresql-firebolt-converter
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

3. **Set up environment variables:**
```bash
cp example.env .env
# Edit .env with your credentials:
# OPENAI_API_KEY=your_openai_api_key_here
```

4. **Run the Flask app:**
```bash
python run.py
```

5. **Open your browser:** `http://localhost:5000`

### Option 2: Run with Streamlit

1. **Follow steps 1-3 above**

2. **Run the Streamlit app:**
```bash
streamlit run streamlit_app.py
```

3. **Open your browser:** `http://localhost:8501`

## 🌐 Deploy to Streamlit Cloud (Free Hosting)

### Step 1: Push to GitHub
```bash
git init
git add .
git commit -m "Initial commit: PostgreSQL to Firebolt converter"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/postgresql-firebolt-converter.git
git push -u origin main
```

### Step 2: Deploy on Streamlit Cloud
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Click "New app"
3. Connect your GitHub repository
4. Set main file path: `streamlit_app.py`
5. Add secrets in "Advanced settings":
   ```toml
   OPENAI_API_KEY = "your_openai_api_key_here"
   ```
6. Click "Deploy"

## 🔧 Configuration

### Required Environment Variables

```env
# OpenAI API key for AI-powered conversion
OPENAI_API_KEY=your_openai_api_key_here

# Optional: Firebolt credentials (can also be entered in UI)
FIREBOLT_CLIENT_ID=your_client_id
FIREBOLT_CLIENT_SECRET=your_client_secret
FIREBOLT_ACCOUNT=your_account
FIREBOLT_DATABASE=your_database
FIREBOLT_ENGINE=your_engine
```

### Firebolt Connection

The tool supports two ways to connect to Firebolt:
1. **UI Form**: Enter credentials directly in the web interface
2. **Environment Variables**: Set credentials in `.env` file

## 📖 How It Works

### 1. **AI-Powered Initial Conversion**
- Sends your PostgreSQL query to OpenAI ChatGPT
- Gets Firebolt-compliant SQL as the first attempt

### 2. **Live Testing**
- Executes the converted query against your real Firebolt database
- Captures actual Firebolt error messages

### 3. **Smart Error Correction**
- **New Errors**: Sends error + query to OpenAI for targeted fixes
- **Repeated Errors**: Tells OpenAI the previous attempts failed and asks for different approaches
- **Intelligent Retry**: Up to 15 attempts with learning from each failure

### 4. **Error Learning**
```
Attempt 1: JSONExtract error → 🆕 NEW ERROR → Standard fix attempt
Attempt 2: Same JSONExtract error → 🔄 REPEATED ERROR → "Previous fix FAILED, try different approach"
Attempt 3: Same error → 🔄 REPEATED ERROR → "Failed in attempts [1,2], completely different method needed"
```

## 🎯 Common PostgreSQL → Firebolt Conversions

| PostgreSQL | Firebolt | Status |
|------------|----------|---------|
| `column->>'path'` | `JSON_POINTER_EXTRACT_TEXT(column, '/path')` | ✅ Auto-fixed |
| `column::json` | `column` (remove cast) | ✅ Auto-fixed |
| `SUM(...) FILTER (WHERE ...)` | `SUM(CASE WHEN ... THEN ... ELSE 0 END)` | ✅ Auto-fixed |
| `now()` | `CURRENT_TIMESTAMP` | ✅ Auto-fixed |
| `EXTRACT(field FROM column)` | `EXTRACT(field FROM CAST(column AS TIMESTAMP))` | ✅ Auto-fixed |
| `column::type` | `CAST(column AS type)` | ✅ Auto-fixed |

## 🔍 Example Usage

**Input PostgreSQL:**
```sql
select sa.applicationid as LAF,
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
order by sa.applicationid
```

**Auto-Corrected Firebolt Output:**
```sql
SELECT sa.applicationid AS LAF,
CASE WHEN sfm.feedesc = 'Login Fees' THEN CAST(saf.totalfeeamount AS DECIMAL) ELSE 0 END AS IMD,
CASE WHEN sfm.feedesc = 'Insurance Penetration Percentage' THEN CAST(saf.totalfeeamount AS DECIMAL) ELSE 0 END AS Total_Insurance,
CASE WHEN sfm.feedesc = 'Processing Fees' THEN CAST(saf.totalfeeamount AS DECIMAL) ELSE 0 END AS Processing_Fees,
'skaleup' AS source,
CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata' AS edl_job_run
FROM skaleup_application sa
LEFT JOIN skaleup_application_fee saf ON sa.applicationkey = saf.applicationkey AND saf.isactive = TRUE
LEFT JOIN skaleup_fee_master sfm ON saf.feetypecode = sfm.feecode AND sfm.isactive = TRUE
WHERE sa.isactive = TRUE
GROUP BY sa.applicationid
ORDER BY sa.applicationid
```

## 🛠️ Development

### Project Structure
```
postgresql-firebolt-converter/
├── app.py                 # Flask web application
├── streamlit_app.py       # Streamlit web application  
├── run.py                 # Flask runner
├── requirements.txt       # Python dependencies
├── converter/             # Core conversion logic
│   ├── query_converter.py # AI-powered query conversion
│   ├── live_tester.py     # Live testing & auto-correction
│   ├── firebolt_client.py # Firebolt database client
│   ├── error_analyzer.py  # Error analysis & categorization
│   └── ...
├── templates/             # Flask HTML templates
└── README.md
```

### Running Tests
```bash
# Test basic conversion
python test_converter.py

# Test with comparison
python comparison_test.py

# Run web comparison tool
python web_comparison.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **OpenAI ChatGPT** for intelligent query conversion
- **Firebolt** for the powerful analytics database
- **Streamlit** for easy deployment and beautiful UI
- All contributors who help improve this tool

## 🆘 Support

If you encounter any issues:

1. Check the [Issues](https://github.com/YOUR_USERNAME/postgresql-firebolt-converter/issues) page
2. Enable detailed logging by setting `DEBUG=True` in your `.env`
3. Review the attempt details in the web interface
4. Create a new issue with your PostgreSQL query and error details

## 🔮 Roadmap

- [ ] Support for more PostgreSQL-specific functions
- [ ] Batch query conversion
- [ ] Query performance optimization suggestions
- [ ] Integration with more SQL databases
- [ ] API endpoints for programmatic access 