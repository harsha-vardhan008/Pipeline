from Extract import extract_text_files
from Transform import transform_data
from Load import load_to_sql  # Or whatever DB you're using

def run_pipeline():
    raw_texts = extract_text_files()  # returns list of strings
    print("Sample resume item:", raw_texts[0])
    structured_df = transform_data(raw_texts)
    load_to_sql(structured_df)

if __name__ == "__main__":
    run_pipeline()
