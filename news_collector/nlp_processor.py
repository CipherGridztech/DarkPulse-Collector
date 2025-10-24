# nlp_processor.py
# Usage: python nlp_processor.py --input raw_articles.jsonl --output processed_articles.jsonl
import argparse, json, time, os, re
from tqdm import tqdm

# HuggingFace pipelines & sentence-transformers
from transformers import pipeline
from sentence_transformers import SentenceTransformer
from bs4 import BeautifulSoup

# --------- Config (change if needed) ----------
SUMMARIZER_MODEL = "facebook/bart-large-cnn"
ZERO_SHOT_MODEL = "facebook/bart-large-mnli"           # zero-shot classifier
NER_MODEL = None  # use default pipeline NER model (let HF pick a reasonable default)
EMBED_MODEL = "all-MiniLM-L6-v2"                      # lightweight embeddings
CANDIDATE_LABELS = [
    "ransomware", "vulnerability", "data breach", "phishing",
    "malware", "scam", "policy", "research", "exposure", "other"
]
ZERO_SHOT_THRESHOLD = 0.30

# --------- Utilities ----------
def clean_html_text(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for bad in soup(["script", "style", "iframe", "aside", "noscript"]):
        bad.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r'\n{2,}', '\n\n', text).strip()
    return text

def safe_write_line(path, obj):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# --------- Load models (do once) ----------
print("Loading models (this may take a while on first run)...")
summarizer = pipeline("summarization", model=SUMMARIZER_MODEL)
classifier = pipeline("zero-shot-classification", model=ZERO_SHOT_MODEL)
ner = pipeline("ner", grouped_entities=True)   # default model; change if you want specific model
embedder = SentenceTransformer(EMBED_MODEL)
print("Models loaded.")

def process_record(rec):
    """
    rec = {"url":..., "title":..., "content": ...}
    returns processed dict
    """
    text = rec.get("content") or rec.get("title") or ""
    text = clean_html_text(text)
    if not text:
        return None

    # summarization (limit length)
    try:
        snippet = " ".join(text.split()[:800])
        summary_out = summarizer(snippet, max_length=120, min_length=20, truncation=True)
        summary = summary_out[0]["summary_text"].strip()
    except Exception as e:
        summary = text[:250]

    # NER
    try:
        ner_input = text[:1000]  # keep small for speed; consider chunking later
        ner_out = ner(ner_input)
        entities = []
        for e in ner_out:
            entities.append({"text": e.get("word") or e.get("entity"), "label": e.get("entity_group") or e.get("entity"), "score": float(e.get("score", 0.0))})
    except Exception:
        entities = []

    # Zero-shot classification
    try:
        cls = classifier(text[:1000], candidate_labels=CANDIDATE_LABELS, multi_label=True)
        labels = []
        for lab, score in zip(cls["labels"], cls["scores"]):
            if score >= ZERO_SHOT_THRESHOLD:
                labels.append({"label": lab, "score": float(score)})
    except Exception:
        labels = []

    # Embedding (vector)
    try:
        emb = embedder.encode(text, show_progress_bar=False)
        embedding = emb.tolist() if hasattr(emb, "tolist") else list(emb)
    except Exception:
        embedding = []

    out = {
        "url": rec.get("url"),
        "title": rec.get("title"),
        "summary": summary,
        "entities": entities,
        "categories": labels,
        "embedding": embedding,
        "scraped_at": rec.get("scraped_at") if rec.get("scraped_at") else None,
        "raw_text_snippet": text[:4000]
    }
    return out

def main(input_file, output_file):
    if not os.path.exists(input_file):
        print("Input file not found:", input_file)
        return

    # optional: remove output if exists to start fresh
    if os.path.exists(output_file):
        print("Appending to existing output:", output_file)
    else:
        print("Creating output file:", output_file)

    with open(input_file, "r", encoding="utf-8") as fin:
        lines = [l.strip() for l in fin if l.strip()]

    print("Records to process:", len(lines))
    for line in tqdm(lines):
        try:
            rec = json.loads(line)
        except:
            continue
        processed = process_record(rec)
        if processed:
            safe_write_line(output_file, processed)
        # tiny delay to avoid spiking CPU/IO
        time.sleep(0.05)

    print("Processing done. Output:", output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="raw_articles.jsonl")
    parser.add_argument("--output", default="processed_articles.jsonl")
    args = parser.parse_args()
    main(args.input, args.output)
