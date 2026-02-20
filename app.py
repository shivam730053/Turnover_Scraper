import streamlit as st

from company_extractor import process_csv_text

st.set_page_config(page_title="Turnover Extractor", layout="centered")
st.title("CSV Turnover Extractor")
st.write("Upload a CSV (`name/city` or `company_name/city`) and download processed output.")

uploaded = st.file_uploader("Input CSV", type=["csv"])

if uploaded is not None:
    try:
        input_text = uploaded.getvalue().decode("utf-8-sig")
    except UnicodeDecodeError:
        st.error("Please upload a UTF-8 CSV file.")
        st.stop()

    if st.button("Process CSV", type="primary"):
        with st.spinner("Processing..."):
            output_text = process_csv_text(input_text)

        st.success("Processing complete")
        st.download_button(
            "Download Output CSV",
            data=output_text.encode("utf-8"),
            file_name="output_extracted.csv",
            mime="text/csv",
        )

        st.subheader("Preview")
        st.code("\n".join(output_text.splitlines()[:12]), language="csv")
