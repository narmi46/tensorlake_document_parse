import streamlit as st
from tensorlake.documentai import (
    DocumentAI,
    ParsingOptions,
    EnrichmentOptions,
    ParseStatus,
    ChunkingStrategy,
    TableOutputMode,
    TableParsingFormat,
    OcrPipelineProvider
)

import json
from bs4 import BeautifulSoup
from tabulate import tabulate
import os
import tempfile
import pandas as pd
import httpx


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
st.title("📄 PDF Extractor (Tensorlake DocumentAI)")
st.write("Upload a PDF and extract **clean text + tables** using Tensorlake.")

api_key = st.text_input("🔑 Enter Tensorlake API Key", type="password")
uploaded_pdf = st.file_uploader("Upload PDF", type=["pdf"])

if uploaded_pdf and api_key:

    if not api_key.startswith("tl_apiKey_"):
        st.error("❌ Invalid API key format.")
        st.stop()

    # -------------------------------------------
    # NEW SETTINGS UI (matches your screenshot)
    # -------------------------------------------

    st.header("⚙️ Parsing Configuration")

    # OCR Model
    ocr_choice = st.radio(
        "OCR model",
        ["model01", "model02", "model03"],
        index=1
    )

    ocr_map = {
        "model01": OcrPipelineProvider.MODEL01,
        "model02": OcrPipelineProvider.MODEL02,
        "model03": OcrPipelineProvider.MODEL03
    }

    # Table Output Mode
    table_output_choice = st.selectbox(
        "Table output mode",
        list(TableOutputMode)
    )

    # Table Parsing Format (rarely used, but available)
    table_parsing_choice = st.selectbox(
        "Table parsing format (internal format)",
        list(TableParsingFormat)
    )

    # Chunking strategy
    chunking_choice = st.selectbox(
        "Chunking strategy",
        list(ChunkingStrategy)
    )

    # Toggles
    cross_page_headers = st.checkbox("Cross page header detection", value=False)
    signature_detection = st.checkbox("Signature detection", value=False)
    remove_strike = st.checkbox("Remove strikethrough lines", value=False)
    skew_detection = st.checkbox("Enable skew detection", value=False)
    disable_layout_detection = st.checkbox("Disable layout detection", value=False)


    st.info("Processing your PDF...")

    # Save temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(uploaded_pdf.read())
        temp_pdf_path = tmp.name

    try:
        doc_ai = DocumentAI(api_key=api_key)

        with st.spinner("📤 Uploading file..."):
            try:
                file_id = doc_ai.upload(temp_pdf_path)
            except httpx.HTTPStatusError as e:
                st.error(f"❌ Upload failed: {e}")
                st.stop()

        # -------------------------------------------
        # Build ParsingOptions with ALL new fields
        # -------------------------------------------

        parsing_options = ParsingOptions(
            chunking_strategy=chunking_choice,
            table_output_mode=table_output_choice,
            table_parsing_format=table_parsing_choice,
            ocr_model=ocr_map[ocr_choice],
            cross_page_header_detection=cross_page_headers,
            signature_detection=signature_detection,
            remove_strikethrough_lines=remove_strike,
            skew_detection=skew_detection,
            disable_layout_detection=disable_layout_detection
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

        for i, chunk in enumerate(result.chunks, start=1):
            st.header(f"📄 Page {i}")

            soup = BeautifulSoup(chunk.content, "html.parser")
            tables = soup.find_all("table")

            for t in tables:
                t.extract()

            text_clean = soup.get_text("\n", strip=True)

            full_text_output += f"\n\n===== PAGE {i} =====\n\n{text_clean}\n\n"
            full_text_with_tables += f"\n\n===== PAGE {i} =====\n\n{text_clean}\n\n"

            st.subheader("📝 Extracted Text")
            st.text(text_clean)

            for t_index, table in enumerate(tables, start=1):
                st.subheader(f"📊 Table {t_index}")

                matrix = html_table_to_matrix(table)
                if not matrix or len(matrix) < 2:
                    st.write("_⚠️ Empty table_")
                    continue

                headers = fix_duplicate_headers(matrix[0])
                df = pd.DataFrame(matrix[1:], columns=headers)
                st.table(df)

                full_text_with_tables += tabulate(matrix[1:], headers=headers, tablefmt="grid") + "\n\n"

                all_tables_json["tables"].append({
                    "page": i,
                    "table_index": t_index,
                    "rows": html_table_to_objects(table),
                })

        st.success("✅ Extraction complete!")

        st.download_button(
            "📥 Download Text (No Tables)",
            full_text_output.encode(),
            file_name="document.txt"
        )

        st.download_button(
            "📥 Download Text + Tables",
            full_text_with_tables.encode(),
            file_name="document_with_tables.txt"
        )

        st.download_button(
            "📥 Download Tables JSON",
            json.dumps(all_tables_json, indent=4).encode(),
            file_name="tables.json"
        )

    finally:
        if os.path.exists(temp_pdf_path):
            os.remove(temp_pdf_path)

else:
    st.warning("Upload a PDF + enter API key to continue.")
