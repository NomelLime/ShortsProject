import streamlit as st
from pipeline import utils, config
import json
import subprocess

st.title("ShortsProject Dashboard")

# Stats from JSON/logs
daily = json.loads(config.DAILY_LIMIT_FILE.read_text()) if config.DAILY_LIMIT_FILE.exists() else {}
st.subheader("Upload Stats")
st.table({k: v.get('uploaded_today', {}) for k, v in daily.items()})

# Accounts
accs = utils.get_all_accounts()
st.subheader("Account Statuses")
for acc in accs:
    st.write(f"{acc['name']}: {', '.join(acc['platforms'])} - Uploads today: {utils.get_uploads_today(acc['dir'])}")

# Errors: Parse log
with open(config.LOG_FILE, 'r') as f:
    errors = [line for line in f if 'ERROR' in line][-10:]
st.subheader("Recent Errors")
st.text("\n".join(errors))

# Run stages
if st.button("Run Full Pipeline"):
    subprocess.run(['python', 'run_pipeline.py'])
    st.success("Pipeline started!")
if st.button("Run Search"):
    subprocess.run(['python', 'run_pipeline.py', '--skip-download', '--skip-processing', '--skip-distribute', '--skip-upload', '--skip-finalize'])
    st.success("Search stage started!")
# Add buttons for other stages similarly

# Table example: Sample stats table
st.subheader("Platform Stats Summary")
data = {
    "Platform": ["YouTube", "TikTok", "Instagram"],
    "Downloaded": [50, 45, 55],
    "Processed": [40, 35, 45],
    "Uploaded": [30, 25, 35]
}
st.table(data)