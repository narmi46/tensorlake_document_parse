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
import httpx


# ------------------ FIXED API KEY ------------------
API_KEY = "YOUR_FIXED_API_KEY_HERE"   # <-- PUT YOUR KEY HERE


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
st.title("📄 Multi-PDF Table & Text Extractor (Tensorlake DocumentAI)")
st.write("Upload **one or many PDFs** and extract clean text + structured tables.")

uploaded_pdfs = st.file_uploader("Upload PDF files", type=["pdf"], accept_multiple_files=True)


if uploaded_pdfs:

    # Initialize DocumentAI once
    doc_ai = DocumentAI(api_key=API_KEY)

    for uploaded_pdf in uploaded_pdfs:

        st.divider()
        st.header(f"📌 Processing: {uploaded_pdf.name}")

        # Save to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(uploaded_pdf.read())
            temp_pdf_path = tmp.name

        try:
            with st.spinner("📤 Uploading..."):
                try:
                    file_id = doc_ai.upload(temp_pdf_path)
                except httpx.HTTPStatusError as e:
                    st.error(f"❌ Upload failed ({e.response.status_code}) for {uploaded_pdf.name}")
                    continue
                except Exception as e:
                    st.error(f"❌ Upload error for {uploaded_pdf.name}: {e}")
                    continue

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

            with st.spinner("🔍 Parsing..."):
                result = doc_ai.parse_and_wait(
                    file_id,
                    parsing_options=parsing_options,
                    enrichment_options=enrichment_options
                )

            if result.status != ParseStatus.SUCCESSFUL:
                st.error(f"❌ Parsing failed: {result.status}")
                continue

            # Prepare file-specific output containers
            text_only = ""
            text_with_tables = ""
            tables_json = {"tables": []}

            # Process pages
            for i, chunk in enumerate(result.chunks, start=1):

                st.subheader(f"📄 Page {i}")

                page_text = chunk.content
                soup = BeautifulSoup(page_text, "html.parser")
                tables = soup.find_all("table")

                # remove HTML table for text extraction
                for tbl in tables:
                    tbl.extract()

                text_clean = soup.get_text("\n", strip=True)

                text_only += f"\n\n===== PAGE {i} =====\n{text_clean}\n"
                text_with_tables += f"\n\n===== PAGE {i} =====\n{text_clean}\n"

                st.text_area("Extracted Text", text_clean, height=150)

                # process tables
                for t_index, table in enumerate(tables, start=1):

                    st.markdown(f"### 📊 Table {t_index}")
                    matrix = html_table_to_matrix(table)

                    if not matrix or len(matrix) < 2:
                        st.write("_No usable table data_")
                        continue

                    headers = fix_duplicate_headers(matrix[0])
                    df = pd.DataFrame(matrix[1:], columns=headers)
                    st.dataframe(df)

                    readable_table = tabulate(matrix[1:], headers=headers, tablefmt="grid")
                    text_with_tables += readable_table + "\n\n"

                    tables_json["tables"].append({
                        "file": uploaded_pdf.name,
                        "page": i,
                        "table_index": t_index,
                        "rows": html_table_to_objects(table)
                    })

            # ------------------ FILE DOWNLOADS ------------------

            st.success(f"✅ Finished: {uploaded_pdf.name}")

            st.download_button(
                f"📥 Download Text (no tables) — {uploaded_pdf.name}",
                data=text_only.encode("utf-8"),
                file_name=f"{uploaded_pdf.name}_text.txt",
                mime="text/plain"
            )

            st.download_button(
                f"📥 Download Text + Tables — {uploaded_pdf.name}",
                data=text_with_tables.encode("utf-8"),
                file_name=f"{uploaded_pdf.name}_with_tables.txt",
                mime="text/plain"
            )

            st.download_button(
                f"📥 Download Tables JSON — {uploaded_pdf.name}",
                data=json.dumps(tables_json, indent=4).encode("utf-8"),
                file_name=f"{uploaded_pdf.name}_tables.json",
                mime="application/json"
            )

        finally:
            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)

else:
    st.warning("Upload one or more PDF files to continue.")
