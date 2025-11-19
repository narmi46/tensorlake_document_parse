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
import httpx  # for catching upload errors


# ------------------ FIXED API KEY ------------------
API_KEY = "tl_apiKey_kqzrz7zrf97fHr7mK8CRh_TsyhVykSJ6XzbQp9LcY_y2Nk2m4-u3"   # <-- PUT YOUR KEY HERE


# ------------------ Helpers ------------------

def clean_number(value):
    try:
        return int(value.replace(",", ""))
    except:
        return value


def fix_duplicate_headers(headers):
    seen = {}
    fixed = []
    for h in headers:
        key = h if h.strip() != "" else "col"
        if key not in seen:
            seen[key] = 1
            fixed.append(key)
        else:
            seen[key] += 1
            fixed.append(f"{key}_{seen[key]}")
    return fixed


def html_table_to_matrix(table):
    rows = table.find_all("tr")
    matrix = []
    for row in rows:
        cells = row.find_all(["td", "th"])
        matrix.append([cell.get_text(strip=True) for cell in cells])
    return matrix


def html_table_to_objects(table):
    matrix = html_table_to_matrix(table)
    if not matrix or len(matrix) < 2:
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
st.title("📄 PDF Table & Text Extractor (Tensorlake)")
st.write("Upload a PDF and extract **clean text + structured tables** using Tensorlake DocumentAI.")

uploaded_pdf = st.file_uploader("Upload PDF file", type=["pdf"])

# ------------------------------------------
# REMOVE API KEY INPUT – ALWAYS USE FIXED KEY
# ------------------------------------------

if uploaded_pdf:

    st.info("Processing your PDF...")

    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(uploaded_pdf.read())
        temp_pdf_path = tmp.name

    try:
        # Init DocumentAI with pre-set API key
        doc_ai = DocumentAI(api_key=API_KEY)

        # Upload file to Tensorlake
        with st.spinner("📤 Uploading file..."):
            try:
                file_id = doc_ai.upload(temp_pdf_path)
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                st.error(f"❌ Upload failed (HTTP {status}): {e}")
                st.stop()
            except Exception as e:
                st.error(f"❌ Upload error: {e}")
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

        with st.spinner("🔍 Parsing PDF..."):
            result = doc_ai.parse_and_wait(
                file_id,
                parsing_options=parsing_options,
                enrichment_options=enrichment_options
            )

        if result.status != ParseStatus.SUCCESSFUL:
            st.error(f"❌ Parsing failed: {result.status}")
            st.stop()

        full_text_output = ""
        full_text_with_tables = ""
        all_tables_json = {"tables": []}

        # Process Pages
        for i, chunk in enumerate(result.chunks, start=1):

            st.header(f"📄 Page {i}")

            page_text = chunk.content
            soup = BeautifulSoup(page_text, "html.parser")
            tables = soup.find_all("table")

            # Remove tables before extracting text
            for tbl in tables:
                tbl.extract()

            text_clean = soup.get_text("\n", strip=True)

            full_text_output += f"\n\n===== PAGE {i} =====\n\n{text_clean}\n\n"
            full_text_with_tables += f"\n\n===== PAGE {i} =====\n\n{text_clean}\n\n"

            st.subheader("📝 Extracted Text")
            st.text(text_clean)

            # Display tables
            for t_index, table in enumerate(tables, start=1):
                st.subheader(f"📊 Table {t_index}")

                matrix = html_table_to_matrix(table)
                if not matrix or len(matrix) < 2:
                    st.write("_⚠️ Empty or malformed table_")
                    continue

                headers = fix_duplicate_headers(matrix[0])
                df = pd.DataFrame(matrix[1:], columns=headers)
                st.table(df)

                readable_table = tabulate(matrix[1:], headers=headers, tablefmt="grid")
                full_text_with_tables += readable_table + "\n\n"

                all_tables_json["tables"].append({
                    "page": i,
                    "table_index": t_index,
                    "rows": html_table_to_objects(table),
                })

        # Downloads
        st.success("✅ Extraction completed!")

        st.download_button(
            "📥 Download Full Text (TXT – no tables)",
            data=full_text_output.encode("utf-8"),
            file_name="document.txt",
            mime="text/plain",
        )

        st.download_button(
            "📥 Download Text + Tables (Readable TXT)",
            data=full_text_with_tables.encode("utf-8"),
            file_name="document_with_tables.txt",
            mime="text/plain",
        )

        st.download_button(
            "📥 Download Tables (JSON)",
            data=json.dumps(all_tables_json, indent=4).encode("utf-8"),
            file_name="tables.json",
            mime="application/json",
        )

    finally:
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)

else:
    st.warning("Upload a PDF to continue.")
