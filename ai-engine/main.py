import os
import re
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from tqdm.auto import tqdm
from sklearn.linear_model import LinearRegression
from flask import Flask, jsonify
import threading

import spacy
from spacy.matcher import PhraseMatcher

# -------------------------------
# 1) CONFIG & DB
# -------------------------------
def connect_db():
    load_dotenv()
    MONGO_URI = os.getenv("MONGO_URI")
    if not MONGO_URI:
        print("❌ MONGO_URI not found. Make sure an .env file exists in ai-engine.")
        raise SystemExit(1)
    client = MongoClient(MONGO_URI)
    db = client.skill_evolution
    print("✅ Database connection successful.")
    return db

# -------------------------------
# 2) HELPERS
# -------------------------------
def extract_skills_batch(texts, nlp_model, matcher_tool, batch_size=512, n_process=1):
    """Batch skill extraction using spaCy nlp.pipe() with progress bar."""
    skills_list = []
    for doc in tqdm(nlp_model.pipe(texts, batch_size=batch_size, n_process=n_process),
                    total=len(texts), desc="Extracting skills"):
        matches = matcher_tool(doc)
        skills = {doc[start:end].text.lower() for _, start, end in matches}
        skills_list.append(list(skills))
    return skills_list

def save_to_db_bulk(collection, doc_id, data_key, data, chunk_size=1000):
    """Save large arrays to MongoDB with progress bar safely."""
    print(f"\n--- Saving {data_key} to database ---")
    try:
        now = pd.Timestamp.now()
        collection.update_one(
            {'_id': doc_id},
            {'$set': {data_key: [], 'last_updated': now}},
            upsert=True
        )
        for i in tqdm(range(0, len(data), chunk_size), desc=f"Saving {data_key}"):
            chunk = data[i:i+chunk_size]
            collection.update_one(
                {'_id': doc_id},
                {'$push': {data_key: {'$each': chunk}}, '$set': {'last_updated': pd.Timestamp.now()}},
                upsert=True
            )
        print(f"✅ {data_key.capitalize()} saved successfully to MongoDB!")
    except Exception as e:
        print(f"❌ Could not save {data_key} to MongoDB. Error: {e}")

def load_historical_frames(historical_data_path):
    print("\n--- Loading historical datasets ---")
    all_dataframes = []
    if not os.path.isdir(historical_data_path):
        print(f"❌ Error: Directory not found at {historical_data_path}")
        raise SystemExit(1)

    for filename in tqdm(os.listdir(historical_data_path), desc="Loading files"):
        if not filename.endswith('.csv'):
            continue

        year_match = re.search(r'_(\d{4})\.csv', filename)
        year = int(year_match.group(1)) if year_match else None
        file_path = os.path.join(historical_data_path, filename)
        print(f"-> Processing {filename} for year {year}...")

        df = pd.read_csv(file_path, engine='python', on_bad_lines='skip')

        # StackOverflow dataset handling
        stack_cols = ['LanguageWorkedWith', 'LanguageDesireNextYear', 'DatabaseWorkedWith', 'DatabaseDesireNextYear']
        if any(col in df.columns for col in stack_cols):
            df['Job Description'] = df[stack_cols].fillna('').agg(' '.join, axis=1)
            df['year'] = year or 2023
            all_dataframes.append(df[['year', 'Job Description']])
            continue

        # Traditional job datasets
        desc_col = next((col for col in ['description', 'Job Description', 'Job_Description']
                         if col in df.columns), None)
        if not desc_col and 'job_skills' in df.columns:
            if 'job_type_skills' in df.columns:
                df['merged_skills'] = df['job_skills'].fillna('') + ' ' + df['job_type_skills'].fillna('')
                desc_col = 'merged_skills'
            else:
                desc_col = 'job_skills'

        if not desc_col:
            print(f"⚠️ Skipping {filename}: No valid description/skills column found.")
            continue

        df.rename(columns={desc_col: 'Job Description'}, inplace=True)
        df['year'] = year or 0
        all_dataframes.append(df[['year', 'Job Description']])

    if not all_dataframes:
        print("❌ No valid CSV files found. Exiting.")
        raise SystemExit(1)

    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"✅ Loaded and combined {len(combined_df)} total job postings.")
    return combined_df

def build_matcher():
    nlp = spacy.blank("en")
    matcher = PhraseMatcher(nlp.vocab, attr='LOWER')
    SKILL_LIST = [
        'python', 'r', 'sql', 'java', 'scala', 'javascript', 'html', 'css',
        'tableau', 'power bi', 'sas', 'excel', 'hadoop', 'spark', 'aws', 'azure', 'gcp',
        'tensorflow', 'pytorch', 'scikit-learn', 'docker', 'kubernetes',
        'react', 'mongodb', 'vue', 'angular', 'typescript'
    ]
    patterns = [nlp.make_doc(skill) for skill in SKILL_LIST]
    matcher.add("SKILL_MATCHER", patterns)
    return nlp, matcher

# -------------------------------
# 3) MAIN PIPELINE
# -------------------------------
def main():
    print("--- Initializing AI Engine ---")
    db = connect_db()
    trends_collection = db.trends
    forecasts_collection = db.forecasts

    historical_data_path = os.path.join('data', 'historical')
    combined_df = load_historical_frames(historical_data_path)

    df_processed = combined_df.copy()
    df_processed.dropna(subset=['Job Description'], inplace=True)

    df_processed['Job Description'] = (
        df_processed['Job Description']
        .astype(str)
        .str.replace(r"<[^>]*>", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.lower()
        .str.strip()
    )

    nlp, matcher = build_matcher()
    is_windows = (os.name == 'nt')
    cpu_count = os.cpu_count() or 2
    n_process = 1 if is_windows else max(1, cpu_count - 1)
    batch_size = 512

    texts = df_processed['Job Description'].tolist()
    df_processed['skills'] = extract_skills_batch(texts, nlp, matcher,
                                                  batch_size=batch_size,
                                                  n_process=n_process)
    print("✅ Skill extraction complete.")

    print("\n--- Calculating and Forecasting Skill Trends ---")
    skills_by_year = df_processed.explode('skills').dropna(subset=['skills'])
    yearly_skill_counts = skills_by_year.groupby(['skills', 'year']).size().reset_index(name='demand_score')

    historical_trends = []
    forecasted_skills = []

    for skill_name, group in tqdm(yearly_skill_counts.groupby('skills'), desc="Forecasting trends"):
        group = group.sort_values('year')
        history = group.to_dict('records')

        if len(history) > 1:
            X = np.array([h['year'] for h in history]).reshape(-1, 1)
            y = np.array([h['demand_score'] for h in history])
            model = LinearRegression().fit(X, y)
            latest_year = history[-1]['year']
            future_years = np.array([latest_year + i for i in range(1, 4)]).reshape(-1, 1)
            predicted_scores = model.predict(future_years)
            forecast = [
                {'year': int(year[0]), 'demand_score': round(max(0, score))}
                for year, score in zip(future_years, predicted_scores)
            ]
            forecasted_skills.append({'skill': skill_name, 'forecast': forecast})

        historical_trends.append({'skill': skill_name, 'history': history})

    save_to_db_bulk(trends_collection, 'skill_historical_trends', 'trends', historical_trends)
    save_to_db_bulk(forecasts_collection, 'skill_forecasts', 'forecasts', forecasted_skills)

# -------------------------------
# ENTRYPOINT
# -------------------------------
if __name__ == "__main__":
    main()


app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "AI Engine is running"}), 200

def run_pipeline():
    # This runs your existing main logic
    main()

if __name__ == "__main__":
    # Start the data processing in a separate thread so the web server can start immediately
    threading.Thread(target=run_pipeline).start()
    # Start the Flask server
    app.run(host='0.0.0.0', port=os.getenv("PORT", 5001))