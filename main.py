import io
import streamlit as st
import fitz  # PyMuPDF
import openpyxl
from io import BytesIO
import zipfile
import pikepdf
import tempfile
import logging
from google.cloud import storage  # GCS integration
from google.oauth2 import service_account  # For service account credentials

# -------------------------------
# Hardcoded Service Account Credentials
# -------------------------------
SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "acquisition-boards-automations",
    "private_key_id": "309fd58ce98ea6c172cf5f9cd2a3b48c41c37da3",
    "private_key": """-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDHj6UW5TiE14jq
PQbJakIQXG1y42n828emyuBJUR1YyIgi2TdDdF/5wvM9Cy7dHIQXJE0Wp/cYTmp6
rih+eEuCofMP94wZHnnuCi/y/h1UxMxcpQLfdnOLR9Ls6lWu9Hc7uXnoBHLrOotq
TuQSG7WOiiuucpmaTpxDq87IPHTB8zJZXWjXO9x6z2/uNDZc1hlql5IWX9k4go0Q
O/3MrxtNhzGzUG9TveiWnPeKqL7N+cS20nE9qAgYk+lqGL5td1MFRKyirj2ld+pY
rygJezKJtgK1d8FjNa1/dIQ6jVwSJiXg4jvlvCjSqciC1h6KPTeuOwgBbwlN8+X9
64G9+VS1AgMBAAECggEABcVkbZFS7VqLUdTSd6wsPWti2eX0OHUFpM/spQokul7Q
OwvDkp6QsPkfjiYe5JOQyVeKKCvS1D/eSe5z6tZhPqWe0RkkUsykE4t1YAZTxFIN
o+c8ukgjZsV8ts+/Cxh0Q0Slnx1T0nQmuHwQEer9uOHLihez0/fOgpF7ISTIbpxj
+tZsXmW0/K6W0Wkj560oOKs0Su87Wt/c12fU5h3q16893XYYHJDgf7biV/XNM6gI
XpurfsI/sFwSDal1Qvu0vebVM4J7XC+6/hIazWpAVFL6JinpI76pCGSBAuHcG5TU
MatFUQp8WtM9Vce1m8+PnX6DYhhjYbYH5hX0kE/uaQKBgQD2gAH1IhIf2GtCpJoP
WJZIbwkw8WKkG+YCvWXNHwwxqpCi8ENUPPGXPo+yngvfel5yEewg90zMetZHGRfU
c7i7oBKSBto9/zCHjRnHWYQ7eNJZm+OKdFwMSJL9ZS9+cI8wpEQ0Zw/Znz9WiwR+
001kn5EHP0gqmjth9uLVFLESuQKBgQDPQIiSoVSjfeCL+YrYqhQiL5qKl/fMlV9b
LhtXSGOaGQHDHBIgo6YMlCRb8RWbbG3D8+EQxAKhjI0eyjs+IoqXUC56D2S2/Y78
KlvjCBf1MtGzAkVBnd2+7HMvDOZTfd77pzIj9BRdV3fLwZSV0VIrL5rbfzgyRBfu
WWGrGwQD3QKBgQCuu16n3VbrrAWcYAG1Dx64ib0CLJm3qu8I0ijvliqWqkmMtrOD
aw/2HirOeqn/6EY6pem0FJkj+Y8bJvZ1avJwTa/cQ29Aszw7WhID9bh+T88MJizN
YF4/dtJ7PNbF0hQubsLKQqRBp1jGiBTPsgkSYunzMTB+woWFk/SHBvveQQKBgQCi
EKS7hMzazCQ7UPfyVY1I7lC67/smT+gxNOzMZB7+8W8fU2QZgd7nFzEXdH6g+zka
cisdISmtimsQGLQa8ofNqzWs3Ty0m7KkHbuc3Udexk6U3MGrffdYxS2NLVkvEM69
mxDqbINAOpXDD61ROk421xMRcXpQVE8iY2KsmoOZQQKBgQCtWchWN7odvrFOAXm/
uP69O7Lm+QU8xArPHpwE3w7cehNaFxn7nX4rK/pzhPW/NdhvwgN5Khla9wCwFy36
mcGWSindegLLcXPQeEtKK5yt80TIDFt47d6Z7FHTgMmwaSv2vksuF0AKIoQDC2Va
QnYl49DOtLl7q1gzKA+pBWnb7g==
-----END PRIVATE KEY-----""",
    "client_email": "pdf-highlighter-tool@acquisition-boards-automations.iam.gserviceaccount.com",
    "client_id": "105085556060102194679",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/pdf-highlighter-tool%40acquisition-boards-automations.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

# Create credentials object from the service account info
credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO)

# -------------------------------
# Initialize Google Cloud Storage Client
# -------------------------------
def initialize_gcs_client():
    """
    Initialize and return a Google Cloud Storage client using hardcoded credentials.
    """
    try:
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        logging.info("Google Cloud Storage client initialized successfully.")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize GCS client: {e}")
        st.error(f"⚠️ Failed to initialize Google Cloud Storage client: {e}")
        return None

# Define bucket name
GCS_BUCKET_NAME = "pdf-highlighter-upload"  # Your bucket name
gcs_client = initialize_gcs_client()
if gcs_client:
    bucket = gcs_client.bucket(GCS_BUCKET_NAME)
    if not bucket.exists():
        st.error(f"⚠️ The bucket '{GCS_BUCKET_NAME}' does not exist.")
        logging.error(f"The bucket '{GCS_BUCKET_NAME}' does not exist.")
    else:
        logging.info(f"Connected to GCS bucket: {GCS_BUCKET_NAME}")

# -------------------------------
# Configure Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    filename='app.log',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# -------------------------------
# Define Preset and General Keywords
# -------------------------------
PRESET_KEYWORDS = {
    "VIC": ["VPA", "Victorian Planning Authority", "VPP", "Victorian Planning Provision"],
    "QLD": ["Shire Planning", "City Planning"],
    "WA": ["Metropolitan Region Scheme", "Rural Living Zone"],
    "NSW": ["Urban Renewal Areas"],
}

GENERAL_KEYWORDS = [
    "Activity Centre", "Amendment", "Amendments Report", "Annual Plan", "Annual Report",
    "Area Plan", "Assessments", "Broadacre", "Budget", "City Plan", "Code Amendment",
    "Concept Plan", "Corporate Business Plan", "Corporate Plan", "Council Action Plan",
    "Council Business Plan", "Council Plan", "Council Report", 
    "Development Investigation Area", "Development Plan", "Development Plan Amendment",
    "DPA", "Emerging community", "Employment land study", "Exhibition", "expansion",
    "Framework", "Framework plan", "Gateway Determination", "greenfield", "growth area", 
    "growth plan", "growth plans", "housing", "Housing Strategy",
    "Industrial land study", "infrastructure plan", "infrastructure planning", 
    "Inquiries", "Investigation area", "land use", "Land use strategy",
    "LDP", "Local Area Plan", "Local Development Area", "Local Development Plan",
    "Local Environmental Plan", "Local Planning Policy", "Local Planning Scheme",
    "Local Planning Strategy", "Local Strategic Planning Statement", "LPP", "LPS", "LSPS",
    "Major Amendment", "Major Update", "Master Plan", "Masterplan", "Neighbourhood Plan",
    "Operational Plan", "Planning Commission", "Planning Framework", "Planning Investigation Area",
    "Planning proposal", "Planning report", "Planning Scheme", "Planning Scheme Amendment",
    "Planning Strategy", "Precinct plan", "Priority Development Area",
    "Project Vision", "Rezoning", "settlement", "Strategy", "Structure Plan", "Structure Planning",
    "Study", "Territory plan", "Town Planning Scheme",
    "Township Plan", "TPS", "Urban Design Framework", "Urban growth", "Urban Release",
    "Urban renewal", "Variation", "Vision", "VPA",
    "Victorian Planning Authority", "VPP", "Victorian Planning Provision",
    "Shire Planning", "City Planning",
    "Metropolitan Region Scheme", "Rural Living Zone",
    "Urban Renewal Areas",
]

# Combine all keywords for easy access
ALL_KEYWORDS = {**PRESET_KEYWORDS, "General": GENERAL_KEYWORDS}

# -------------------------------
# Initialize Streamlit Session State
# -------------------------------
if 'updated_pdfs' not in st.session_state:
    st.session_state.updated_pdfs = {}
if 'csv_reports' not in st.session_state:
    st.session_state.csv_reports = {}
if 'selected_keywords' not in st.session_state:
    st.session_state.selected_keywords = set()

# -------------------------------
# Callback Functions
# -------------------------------
def select_all_callback():
    """
    Callback function for 'Select All Keywords' checkbox.
    """
    if st.session_state.select_all_keywords:
        st.session_state.selected_keywords = set([kw for kws in ALL_KEYWORDS.values() for kw in kws])
    else:
        st.session_state.selected_keywords = set()

def toggle_state_callback(state):
    """
    Callback function for state-specific keyword checkboxes.
    """
    state_key = f'state_{state}'
    if st.session_state[state_key]:
        st.session_state.selected_keywords.update(PRESET_KEYWORDS[state])
    else:
        st.session_state.selected_keywords.difference_update(PRESET_KEYWORDS[state])

def preprocess_pdf_with_pikepdf(input_stream):
    """
    Attempt to preprocess the PDF using pikepdf.
    """
    try:
        pdf = pikepdf.open(input_stream)
        output = io.BytesIO()
        pdf.save(output)
        pdf.close()
        output.seek(0)
        logging.info("Successfully preprocessed PDF with pikepdf.")
        return output
    except pikepdf.PdfError as e:
        logging.error(f"pikepdf preprocessing failed: {e}")
        st.error(f"⚠️ Failed to preprocess PDF with pikepdf: {e}")
        return None

# -------------------------------
# Helper Functions
# -------------------------------

def is_valid_pdf(file):
    """
    Validate if the uploaded file is a valid, unencrypted PDF.
    """
    try:
        file.seek(0)
        with fitz.open(stream=file.read(), filetype="pdf") as doc:
            if doc.is_encrypted:
                st.error(f"⚠️ {file.name} is encrypted. Please provide an unencrypted PDF.")
                return False
            return True
    except Exception as e:
        st.error(f"⚠️ {file.name} is not a valid PDF file. Error: {e}")
        logging.error(f"Error validating PDF {file.name}: {e}")
        return False

def upload_to_gcs(blob_name, file_data):
    """
    Upload a file to Google Cloud Storage.
    """
    if not gcs_client or not bucket:
        st.error("⚠️ Google Cloud Storage client is not initialized.")
        return None
    try:
        blob = bucket.blob(blob_name)
        blob.upload_from_file(file_data, rewind=True)
        # blob.make_public()  # Removed to comply with UBLA
        logging.info(f"Uploaded {blob_name} to GCS.")
        return blob.public_url  # Note: This URL may not be publicly accessible
    except Exception as e:
        logging.error(f"Failed to upload {blob_name} to GCS: {e}")
        st.error(f"⚠️ Failed to upload {blob_name} to Google Cloud Storage: {e}")
        return None

def generate_signed_url(blob_name, expiration=3600):
    """
    Generate a signed URL for a blob.
    :param blob_name: Name of the blob in GCS.
    :param expiration: Time in seconds for the URL to remain valid.
    :return: Signed URL as a string.
    """
    if not gcs_client or not bucket:
        st.error("⚠️ Google Cloud Storage client is not initialized.")
        return None
    try:
        blob = bucket.blob(blob_name)
        url = blob.generate_signed_url(expiration=expiration)
        logging.info(f"Generated signed URL for {blob_name}: {url}")
        return url
    except Exception as e:
        logging.error(f"Failed to generate signed URL for {blob_name}: {e}")
        st.error(f"⚠️ Failed to generate signed URL for {blob_name}: {e}")
        return None

def highlight_text_in_pdf(file_content, selected_keywords, original_filename):
    """
    Highlight selected keywords in the PDF and return the updated PDF and keyword occurrences.
    Includes preprocessing steps for corrupted or complex PDFs.
    Also uploads original and processed PDFs to GCS.
    """
    # Attempt to open the PDF with PyMuPDF
    try:
        pdf_file = io.BytesIO(file_content)
        pdf_document = fitz.open(stream=pdf_file, filetype="pdf")
    except fitz.fitz.FileDataError as e:
        st.warning(f"⚠️ {original_filename} has structural issues. Attempting to preprocess with pikepdf...")
        logging.warning(f"{original_filename} has structural issues: {e}")

        # Attempt preprocessing with pikepdf
        preprocessed_pdf = preprocess_pdf_with_pikepdf(io.BytesIO(file_content))
        
        if preprocessed_pdf:
            try:
                pdf_document = fitz.open(stream=preprocessed_pdf, filetype="pdf")
                st.success(f"✅ Successfully preprocessed {original_filename} with pikepdf.")
            except Exception as e:
                st.error(f"⚠️ Failed to process {original_filename} even after preprocessing. Error: {e}")
                logging.error(f"Failed to open preprocessed PDF with pikepdf for {original_filename}: {e}")
                return None, None
        else:
            st.error(f"⚠️ Failed to preprocess {original_filename} with pikepdf.")
            return None, None

    except Exception as e:
        st.error(f"⚠️ An unexpected error occurred while opening {original_filename}: {e}")
        logging.error(f"Unexpected error opening {original_filename}: {e}")
        return None, None

    # Upload original PDF to GCS
    original_blob_name = f"original_pdfs/{original_filename}"
    original_pdf_stream = io.BytesIO(file_content)
    original_pdf_stream.seek(0)
    original_pdf_url = upload_to_gcs(original_blob_name, original_pdf_stream)
    if original_pdf_url:
        logging.info(f"Original PDF uploaded to GCS: {original_pdf_url}")

    # Initialize keyword_occurrences with all selected keywords
    keyword_occurrences = {keyword: [] for keyword in selected_keywords}
    keywords_found = False

    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text = page.get_text("dict")

        for keyword in selected_keywords:
            keyword_lower = keyword.lower()

            for block in text["blocks"]:
                if block["type"] == 0:  # Block is text
                    for line in block["lines"]:
                        for span in line["spans"]:
                            text_content = span["text"]
                            lower_text = text_content.lower()

                            start = 0
                            while True:
                                start = lower_text.find(keyword_lower, start)
                                if start == -1:
                                    break

                                # Track page number for each keyword occurrence
                                if (page_num + 1) not in keyword_occurrences[keyword]:
                                    keyword_occurrences[keyword].append(page_num + 1)
                                    keywords_found = True  # At least one keyword found

                                # Highlight the keyword in the PDF
                                bbox = span["bbox"]
                                span_width = bbox[2] - bbox[0]
                                span_height = bbox[3] - bbox[1]
                                char_width = span_width / len(text_content) if len(text_content) > 0 else 1

                                keyword_bbox = fitz.Rect(
                                    bbox[0] + char_width * start,
                                    bbox[1],
                                    bbox[0] + char_width * (start + len(keyword)),
                                    bbox[3]
                                )
                                
                                keyword_bbox = keyword_bbox.intersect(fitz.Rect(0, 0, page.rect.width, page.rect.height))
                                
                                if not keyword_bbox.is_empty:
                                    highlight = page.add_highlight_annot(keyword_bbox)
                                    highlight.set_colors(stroke=(1, 0.65, 0))  # Set color to orange
                                    highlight.update()

                                start += len(keyword)

    # Save the highlighted PDF
    output_pdf = BytesIO()
    try:
        pdf_document.save(output_pdf)
        output_pdf.seek(0)
    except Exception as e:
        st.error(f"⚠️ Failed to save highlighted PDF for {original_filename}: {e}")
        logging.error(f"Failed to save highlighted PDF for {original_filename}: {e}")
        return None, None
    finally:
        pdf_document.close()

    # Upload processed PDF to GCS
    processed_blob_name = f"processed_pdfs/highlighted_{original_filename}"
    processed_pdf_stream = BytesIO(output_pdf.getvalue())
    processed_pdf_stream.seek(0)
    processed_pdf_url = upload_to_gcs(processed_blob_name, processed_pdf_stream)
    if processed_pdf_url:
        logging.info(f"Processed PDF uploaded to GCS: {processed_pdf_url}")

    if not keywords_found:
        return output_pdf, None  # Return the updated PDF even if no keywords are found

    return output_pdf, keyword_occurrences

def generate_csv_report(keyword_occurrences, original_filename):
    """
    Generate a CSV report from keyword occurrences.
    Also uploads the report to GCS.
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Keywords Report"

    # Write header
    ws.append(["Keyword", "Occurrences (Page Numbers)"])

    # Write keyword occurrences
    for keyword, pages in keyword_occurrences.items():
        if pages:
            ws.append([keyword, ", ".join(map(str, pages))])

    # Auto-size columns
    for column_cells in ws.columns:
        length = max(len(str(cell.value)) for cell in column_cells)
        ws.column_dimensions[column_cells[0].column_letter].width = length + 2

    # Save to BytesIO
    excel_output = BytesIO()
    wb.save(excel_output)
    excel_output.seek(0)

    # Upload CSV report to GCS
    report_filename = f"keywords_report_{original_filename.replace('.pdf', '.xlsx')}"
    report_blob_name = f"reports/{report_filename}"
    report_stream = BytesIO(excel_output.getvalue())
    report_stream.seek(0)
    report_url = upload_to_gcs(report_blob_name, report_stream)
    if report_url:
        logging.info(f"CSV report uploaded to GCS: {report_url}")

    return excel_output

def hide_file_uploader_instructions():
    """
    Hide the default file uploader instructions using custom CSS.
    """
    hide_ui = """
                <style>
                /* Hide the default drag and drop instructions and size limit */
                div[data-testid="stFileUploadDropzone"] > div > div > div > p {
                    display: none;
                }
                div[data-testid="stFileUploadDropzone"] > div > div > div > span {
                    display: none;
                }
                </style>
                """
    st.markdown(hide_ui, unsafe_allow_html=True)

# -------------------------------
# Main Tool Interface
# -------------------------------
def keyword_highlighter_page():
    st.title("📄 PDF Keyword Highlighter")

    st.write("📂 **Instructions:**")
    st.write("- **Upload PDFs:** Click the upload button and select multiple PDF files by holding `Ctrl` or `Shift`.")
    st.write("- **Select Keywords:** Choose from the predefined keywords or add your own.")
    st.write("- **Process:** Click the 'Highlight Keywords' button to start processing.")

    MAX_FILES = 20  # Maximum number of files
    MAX_TOTAL_SIZE_MB = 5000  # Maximum total upload size in MB

    # Hide default file uploader instructions
    hide_file_uploader_instructions()

    uploaded_files = st.file_uploader(
        f"📂 Choose up to {MAX_FILES} PDF files (Total size up to {MAX_TOTAL_SIZE_MB} MB)",
        type="pdf",
        accept_multiple_files=True
    )

    if uploaded_files:
        # Enforce the maximum number of files
        if len(uploaded_files) > MAX_FILES:
            st.error(f"⚠️ You can upload a maximum of {MAX_FILES} files at once.")
            uploaded_files = uploaded_files[:MAX_FILES]

        # Calculate the total size
        total_size = sum(file.size for file in uploaded_files) / (1024 * 1024)  # Convert to MB
        if total_size > MAX_TOTAL_SIZE_MB:
            st.error(f"⚠️ The total upload size exceeds {MAX_TOTAL_SIZE_MB} MB. Please reduce the number or size of files.")
            # Trim the list to fit the size limit
            allowed_size = 0
            valid_files = []
            for file in uploaded_files:
                file_size_mb = file.size / (1024 * 1024)
                if allowed_size + file_size_mb <= MAX_TOTAL_SIZE_MB:
                    valid_files.append(file)
                    allowed_size += file_size_mb
                else:
                    st.warning(f"⚠️ {file.name} exceeds the remaining upload size limit and was skipped.")
            uploaded_files = valid_files

        # Display uploaded files
        st.write(f"📥 **Uploaded {len(uploaded_files)} files:**")
        for uploaded_file in uploaded_files:
            st.write(f"- {uploaded_file.name} ({uploaded_file.size / (1024 * 1024):.2f} MB)")

        st.subheader("🔍 Select Keywords to Highlight")

        # Determine if all keywords are selected
        all_keywords_set = set([kw for kws in ALL_KEYWORDS.values() for kw in kws])
        select_all_checked = all_keywords_set.issubset(st.session_state.selected_keywords)

        # Add a "Select All" checkbox with callback
        select_all = st.checkbox("✅ Select All Keywords", value=select_all_checked, key="select_all_keywords", on_change=select_all_callback)

        # Display state tickboxes under "States:" sub-section with callbacks
        st.markdown("### States:")
        for state, keywords in PRESET_KEYWORDS.items():
            state_key = f'state_{state}'
            # Determine if all keywords for the state are selected
            state_keywords_set = set(keywords)
            state_checked = state_keywords_set.issubset(st.session_state.selected_keywords)
            is_checked = st.checkbox(f"✅ {state}", value=state_checked, key=state_key, on_change=toggle_state_callback, args=(state,))

        # General category (includes state-specific keywords) distributed across 4 columns
        with st.expander("### General Keywords", expanded=False):
            col1, col2, col3, col4 = st.columns(4)
            columns = [col1, col2, col3, col4]
            for i, keyword in enumerate(GENERAL_KEYWORDS):
                col = columns[i % 4]
                checkbox_key = f"General_{keyword}"
                is_checked = keyword in st.session_state.selected_keywords
                if col.checkbox(keyword, value=is_checked, key=checkbox_key):
                    st.session_state.selected_keywords.add(keyword)
                else:
                    st.session_state.selected_keywords.discard(keyword)

        # Custom keyword addition
        custom_keywords = st.text_area("✏️ Or add your own keywords (one per line):", "")
        if custom_keywords:
            custom_keywords_list = [kw.strip() for kw in custom_keywords.split('\n') if kw.strip()]
            st.session_state.selected_keywords.update(custom_keywords_list)

        # Add a checkbox for optional CSV report
        generate_csv = st.checkbox("📊 Generate CSV Report", value=False, key="generate_csv_report")

        if st.button("🚀 Highlight Keywords"):
            if not st.session_state.selected_keywords:
                st.error("⚠️ Please select or add at least one keyword.")
            else:
                # Validate uploaded files
                valid_files = []
                for file in uploaded_files:
                    # Reset the file pointer before validation
                    file.seek(0)
                    if is_valid_pdf(file):
                        valid_files.append(file)
                    else:
                        st.error(f"⚠️ {file.name} is not a valid PDF file.")
                if not valid_files:
                    st.error("⚠️ No valid PDF files to process.")
                else:
                    # Clear previous results
                    st.session_state.updated_pdfs = {}
                    st.session_state.csv_reports = {}
                    
                    total_files = len(valid_files)
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for idx, uploaded_file in enumerate(valid_files):
                        # Reset the file pointer before reading
                        uploaded_file.seek(0)
                        
                        # Read the file content once
                        file_content = uploaded_file.read()
                        if not file_content:
                            st.error(f"⚠️ {uploaded_file.name} is empty after reading.")
                            continue
                        
                        # Update status text
                        status_text.text(f"Processing file {idx + 1} of {total_files}: {uploaded_file.name}")
                        
                        # Process each PDF
                        updated_pdf, keyword_occurrences = highlight_text_in_pdf(
                            file_content, st.session_state.selected_keywords, uploaded_file.name
                        )

                        if not updated_pdf:
                            st.warning(f"⚠️ {uploaded_file.name} could not be processed.")
                            continue

                        if not keyword_occurrences:
                            st.warning(f"No keywords found in **{uploaded_file.name}**.")
                            # Still store the PDF even if no keywords are found
                            st.session_state.updated_pdfs[uploaded_file.name] = updated_pdf
                            continue

                        # Store the updated PDF in session state
                        st.session_state.updated_pdfs[uploaded_file.name] = updated_pdf

                        # Generate CSV report if checkbox is selected
                        if generate_csv:
                            csv_report = generate_csv_report(keyword_occurrences, uploaded_file.name)
                            st.session_state.csv_reports[uploaded_file.name] = csv_report

                        # Update progress bar
                        progress = (idx + 1) / total_files
                        progress_bar.progress(progress)

# -------------------------------
# Download Section
# -------------------------------
def download_section():
    if st.session_state.updated_pdfs:
        st.success("✅ Processing complete!")
        num_pdfs = len(st.session_state.updated_pdfs)
        
        if num_pdfs > 1:
            # More than one PDF, provide "Download All PDFs as ZIP"
            st.write("📥 **Download All Updated PDFs:**")
            # Create a zip file of all updated PDFs
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                for filename, pdf in st.session_state.updated_pdfs.items():
                    zip_file.writestr(f"highlighted_{filename}", pdf.getvalue())
            zip_buffer.seek(0)
            st.download_button(
                label="📄 Download All PDFs as ZIP",
                data=zip_buffer,
                file_name="highlighted_pdfs.zip",
                mime="application/zip",
                key="download_all_pdfs"
            )
        else:
             # Only one PDF in updated_pdfs
            st.write("📥 **Download Updated PDF:**")
            # Extract the single PDF from the dictionary
            (filename, pdf_stream) = list(st.session_state.updated_pdfs.items())[0]

            # Provide a direct download button
            st.download_button(
                label=f"📄 Download {filename}",
                data=pdf_stream.getvalue(),            # The PDF bytes in memory
                file_name=f"highlighted_{filename}",   # Name to show in "Save As..."
                mime="application/pdf"
            )
        
        # CSV Reports
        if st.session_state.csv_reports:
            if len(st.session_state.csv_reports) > 1:
                st.write("📊 **Download All CSV Reports:**")
                # Create a zip file of all CSV reports
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                    for filename, csv_report in st.session_state.csv_reports.items():
                        report_filename = f"keywords_report_{filename.replace('.pdf', '.xlsx')}"
                        zip_file.writestr(report_filename, csv_report.getvalue())
                zip_buffer.seek(0)
                st.download_button(
                    label="📄 Download All Reports as ZIP",
                    data=zip_buffer,
                    file_name="keywords_reports.zip",
                    mime="application/zip",
                    key="download_all_reports"
                )
            else:
                st.write("📊 **Download CSV Report:**")
                # Single report, provide individual download button
                for filename, csv_report in st.session_state.csv_reports.items():
                    signed_url = generate_signed_url(f"reports/{filename.replace('.pdf', '.xlsx')}")
                    if signed_url:
                        st.markdown(f"🔗 [Download {filename} Report](<{signed_url}>)")

# -------------------------------
# Main Function
# -------------------------------
def main():
    keyword_highlighter_page()
    download_section()

# Execute the main function
main()
