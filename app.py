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
API_KEY = "tl_apiKey_kqzrz7zrf97fHr7mK8CRh_TsyhVykSJ6XzbQp9LcY_y2Nk2m4-u3"   # <-- insert your real key here


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

            elif h_low in ["2022023", "as restated 2023", "year_2023", "2023"]:
                entry["year_2023"] = clean_number(v)

            elif h_low == "note":
                entry["note"] = clean_number(v) if v else None

            else:
                entry["name"] = v

        objects.append(entry)

    return objects



# ------------------ Streamlit UI ------------------

st.set_page_config(page_title="PDF Parser", layout="wide")

st.title("📄 Multi-PDF Extractor (Tensorlake DocumentAI)")
st.write("Upload **one or more PDFs** to extract clean text and structured tables automatically.")


uploaded_pdfs = st.file_uploader(
    "Upload PDF files",
    type=["pdf"],
    accept_multiple_files=True
)


# ------------------ MAIN PROCESSING ------------------

if uploaded_pdfs:

    doc_ai = DocumentAI(api_key=API_KEY)

    for uploaded_pdf in uploaded_pdfs:

        st.divider()
        st.header(f"📌 Processing File: `{uploaded_pdf.name}`")

        # Save file to temp
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(uploaded_pdf.read())
            temp_pdf_path = tmp.name

        try:
            with st.spinner("📤 Uploading to Tensorlake..."):
                try:
                    file_id = doc_ai.upload(temp_pdf_path)
                except httpx.HTTPStatusError as e:
                    st.error(f"❌ Upload failed ({e.response.status_code})")
                    continue
                except Exception as e:
                    st.error(f"❌ Upload error: {e}")
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

            with st.spinner("🔍 Parsing PDF..."):
                result = doc_ai.parse_and_wait(
                    file_id,
                    parsing_options=parsing_options,
                    enrichment_options=enrichment_options
                )

            if result.status != ParseStatus.SUCCESSFUL:
                st.error(f"❌ Parsing failed: {result.status}")
                continue

            # Outputs per file
            text_only = ""
            text_with_tables = ""
            tables_json = {"tables": []}

            # Process each page
            for i, chunk in enumerate(result.chunks, start=1):

                st.subheader(f"📄 Page {i}")

                page_text = chunk.content
                soup = BeautifulSoup(page_text, "html.parser")
                tables = soup.find_all("table")

                # Extract clean text
                for tbl in tables:
                    tbl.extract()

                clean_text = soup.get_text("\n", strip=True)

                text_only += f"\n===== PAGE {i} =====\n{clean_text}\n"
                text_with_tables += f"\n===== PAGE {i} =====\n{clean_text}\n"

                st.text_area("📝 Extracted Text", clean_text, height=150)

                # Process tables
                for t_index, table in enumerate(tables, start=1):

                    st.markdown(f"### 📊 Table {t_index}")

                    matrix = html_table_to_matrix(table)
                    if not matrix or len(matrix) < 2:
                        st.write("_No valid table found_")
                        continue

                    headers = fix_duplicate_headers(matrix[0])
                    df = pd.DataFrame(matrix[1:], columns=headers)
                    st.dataframe(df)

                    readable = tabulate(matrix[1:], headers=headers, tablefmt="grid")
                    text_with_tables += readable + "\n\n"

                    tables_json["tables"].append({
                        "file": uploaded_pdf.name,
                        "page": i,
                        "table_index": t_index,
                        "rows": html_table_to_objects(table)
                    })

            # ------------------ DOWNLOAD BUTTONS ------------------
            st.success(f"✔ Completed: {uploaded_pdf.name}")

            st.download_button(
                f"📥 Download Text Only — {uploaded_pdf.name}",
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
            # Cleanup temp
            if os.path.exists(temp_pdf_path):
                os.remove(temp_pdf_path)

else:
    st.info("Upload one or more PDF files to begin.")
