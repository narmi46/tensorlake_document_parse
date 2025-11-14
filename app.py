import streamlit as st
from tensorlake.documentai import (
    DocumentAI,
    ParsingOptions,
    EnrichmentOptions,
    ParseStatus,
    ChunkingStrategy,
    TableOutputMode
)

import json
from bs4 import BeautifulSoup
from tabulate import tabulate
import os
import tempfile
import pandas as pd
import httpx  # <-- NEW: for catching HTTPStatusError


# ------------------ Helpers ------------------

def clean_number(value):
    """Convert strings like '4,947,807' → 4947807."""
    try:
        return int(value.replace(",", ""))
    except:
        return value


def html_table_to_matrix(table):
    """Convert HTML <table> → matrix of rows."""
    rows = table.find_all("tr")
    matrix = []
    for row in rows:
        cells = row.find_all(["td", "th"])
        matrix.append([cell.get_text(strip=True) for cell in cells])
    return matrix


def html_table_to_objects(table):
    """Convert HTML table to structured dictionaries for JSON."""
    matrix = html_table_to_matrix(table)
    if not matrix:
        return []

    header = matrix[0]
    objects = []

    for row in matrix[1:]:
        entry = {}
        for h, v in zip(header, row):

            h_low = h.lower().strip()

            if h_low in ["2024", "year_2024"]:
                entry["year_2024"] = clean_number(v)

            elif h_low in ["2023", "as restated 2023", "year_2023"]:
                entry["year_2023"] = clean_number(v)

            elif h_low == "note":
                entry["note"] = clean_number(v) if v else None

            else:
                entry["name"] = v

        objects.append(entry)

    return objects


# ------------------ Streamlit UI ------------------

st.set_page_config(page_title="PDF Parser", layout="wide")
st.title("📄 PDF Table & Text Extractor")
st.write("Upload a PDF and extract **clean text + tables** using Tensorlake DocumentAI.")

api_key = st.text_input("🔑 Enter Tensorlake API Key", type="password")

uploaded_pdf = st.file_uploader("Upload PDF file", type=["pdf"])

if uploaded_pdf and api_key:
    # --- Basic API key format validation ---
    if not api_key.startswith("tl_apiKey_"):
        st.error("❌ The API key does not look like a valid Tensorlake key (should start with `tl_apiKey_`).")
        st.stop()

    st.info("Processing your PDF...")

    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(uploaded_pdf.read())
        temp_pdf_path = tmp.name

    try:
        # Init DocumentAI
        doc_ai = DocumentAI(api_key=api_key)

        # Upload file with error handling
        with st.spinner("📤 Uploading file to Tensorlake..."):
            try:
                file_id = doc_ai.upload(temp_pdf_path)
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status in (401, 403):
                    st.error("❌ Authentication failed (HTTP "
                             f"{status}). Please check that your API key is correct and has access rights.")
                else:
                    st.error(f"❌ File upload failed with HTTP {status}: {e}")
                st.stop()
            except Exception as e:
                st.error(f"❌ Unexpected error during upload: {e}")
                st.stop()

        # Configure parsing
        parsing_options = ParsingOptions(
            chunking_strategy=ChunkingStrategy.PAGE,
            table_output_mode=TableOutputMode.HTML,
            signature_detection=False,
            disable_layout_detection=True,
        )

        enrichment_options = EnrichmentOptions(
            figure_summarization=False,
            table_summarization=False
        )

        # Parse
        with st.spinner("🔍 Parsing PDF with DocumentAI..."):
            result = doc_ai.parse_and_wait(
                file_id,
                parsing_options=parsing_options,
                enrichment_options=enrichment_options
            )

        if result.status != ParseStatus.SUCCESSFUL:
            st.error(f"❌ Parsing failed: {result.status}")
            st.stop()

        # Storage for outputs
        full_text_output = ""
        all_tables_json = {"tables": []}

        # ------------------ Process Pages ------------------
        for i, chunk in enumerate(result.chunks, start=1):

            st.header(f"📄 Page {i}")

            page_text = chunk.content
            soup = BeautifulSoup(page_text, "html.parser")
            tables = soup.find_all("table")

            # Remove tables from page text so we don't see raw table text
            for tbl in tables:
                tbl.extract()

            text_without_tables = soup.get_text("\n", strip=True)
            full_text_output += f"\n\n===== PAGE {i} =====\n\n{text_without_tables}\n\n"

            st.subheader("📝 Extracted Text")
            st.text(text_without_tables)

            # Display tables
            for t_index, table in enumerate(tables, start=1):
                st.subheader(f"📊 Table {t_index}")

                matrix = html_table_to_matrix(table)
                if not matrix or len(matrix) < 2:
                    st.write("_Empty or malformed table_")
                    continue

                df = pd.DataFrame(matrix[1:], columns=matrix[0])
                st.table(df)

                # For JSON export
                table_obj = {
                    "page": i,
                    "table_index": t_index,
                    "rows": html_table_to_objects(table),
                }
                all_tables_json["tables"].append(table_obj)

        # ------------------ Provide Downloads ------------------
        txt_bytes = full_text_output.encode("utf-8")
        json_bytes = json.dumps(all_tables_json, indent=4).encode("utf-8")

        st.download_button(
            label="📥 Download Extracted Text (TXT)",
            data=txt_bytes,
            file_name="document.txt",
            mime="text/plain"
        )

        st.download_button(
            label="📥 Download Tables (JSON)",
            data=json_bytes,
            file_name="tables.json",
            mime="application/json"
        )

    finally:
        # Clean up temp file
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)

else:
    st.warning("Please upload a PDF and provide your API key.")
