import streamlit as st
import pandas as pd
import requests
import io
from io import StringIO

# ------------------- CONFIG -------------------
FASTAPI_URL = "http://127.0.0.1:8000"
st.set_page_config(page_title="CSV Merger - FastAPI + Streamlit", layout="wide")

st.title("📂 CSV Merge App (Manual Upload + FastAPI Save)")
st.caption("Upload two CSV files, enter join column and join type, merge locally, and optionally upload to MinIO via FastAPI.")

# ------------------- UPLOAD SECTION -------------------
st.header(" Upload Your CSV Files")

col1, col2 = st.columns(2)
with col1:
    file1 = st.file_uploader("Upload First CSV", type=["csv"], key="file1")
with col2:
    file2 = st.file_uploader("Upload Second CSV", type=["csv"], key="file2")

if file1 and file2:
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    st.subheader(" Preview of Uploaded Files")
    st.write("**File 1**", df1.head())
    st.write("**File 2**", df2.head())

    # ------------------- MERGE SECTION -------------------
    st.header(" Merge Options")

    join_column = st.text_input("Enter the common column name (case-insensitive)", value="customer_id")
    join_type = st.selectbox("Select Join Type", ["inner", "left", "right", "outer"])

    if st.button(" Merge Files"):
        join_column = join_column.strip().lower()
        df1.columns = [col.lower().strip() for col in df1.columns]
        df2.columns = [col.lower().strip() for col in df2.columns]

        if join_column not in df1.columns or join_column not in df2.columns:
            st.error(f" '{join_column}' not found in both files.\nFile1 columns: {list(df1.columns)}\nFile2 columns: {list(df2.columns)}")
        else:
            merged_df = pd.merge(df1, df2, on=join_column, how=join_type)
            st.success(f" Files merged successfully using **{join_type}** join on **{join_column}**")
            st.dataframe(merged_df.head(10))

            # Save merged CSV in session for later
            csv_buffer = io.StringIO()
            merged_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            st.download_button(
                label="⬇ Download Merged CSV",
                data=csv_data,
                file_name=f"merged_{join_type}_join.csv",
                mime="text/csv"
            )

            # ------------------- UPLOAD TO FASTAPI -------------------
            st.header("3️⃣ Save Merged File to FastAPI (MinIO + PostgreSQL)")

            if st.button("💾 Upload to FastAPI Backend"):
                files = {"file": ("merged_file.csv", csv_data, "text/csv")}
                data = {
                    "uploaded_by": "Debangshu",
                    "description": f"Merged via Streamlit using {join_type} join"
                }
                res = requests.post(f"{FASTAPI_URL}/upload", files=files, data=data)

                if res.status_code == 200:
                    st.success("✅ Merged file successfully uploaded to FastAPI (MinIO + PostgreSQL)")
                    st.json(res.json())
                else:
                    st.error(f"❌ Upload failed: {res.text}")
else:
    st.info("👆 Please upload two CSV files to start.")
streamlit run frontend.py
