import csv
import os
import argparse
from dotenv import load_dotenv
from chatbot_engine import ChatbotEngine

try:
    load_dotenv(encoding='utf-8')
except Exception as e:
    print(f"⚠️ Warning: could not load .env file: {e}")


def load_papers(csv_path):
    papers = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            normalized = [fn.strip().lstrip('\ufeff') for fn in reader.fieldnames]
            if normalized != reader.fieldnames:
                for raw in reader:
                    newrow = {}
                    for oldk, newk in zip(reader.fieldnames, normalized):
                        newrow[newk] = raw.get(oldk)
                    papers.append(newrow)
            else:
                for row in reader:
                    papers.append(row)
        else:
            return []
    return papers


def main():
    parser = argparse.ArgumentParser(description='Run chatbot against SB_publications.csv')
    parser.add_argument('--paper', '-p', help='Paper index (1-based) or exact title', default=None)
    parser.add_argument('--question', '-q', help='Question to ask about the paper', default=None)
    args = parser.parse_args()

    base = os.path.dirname(__file__)
    csv_path = os.path.join(base, 'SB_publications.csv')

    papers = load_papers(csv_path)
    if not papers:
        print('No papers found in the CSV')
        return

    sample = papers[0]
    possible_title_keys = ['Title', 'title', 'T\u00edtulo', 'Título', 'titulo']
    possible_link_keys = ['Link', 'link', 'URL', 'Url']
    title_key = next((k for k in possible_title_keys if k in sample and sample.get(k)), None)
    link_key = next((k for k in possible_link_keys if k in sample and sample.get(k)), None)
    if not title_key:
        title_key = list(sample.keys())[0]
    if not link_key:
        link_key = list(sample.keys())[1] if len(sample.keys()) > 1 else list(sample.keys())[0]

    print(f'Found {len(papers)} papers. Listing first 20:')
    for i, p in enumerate(papers[:20], 1):
        print(f"{i}. {p.get(title_key)} -> {p.get(link_key)}")

    if args.paper:
        sel = args.paper
        if sel.isdigit():
            idxn = int(sel) - 1
            if idxn < 0 or idxn >= len(papers):
                print('Index out of range')
                return
            paper_title = papers[idxn].get(title_key)
        else:
            paper_title = sel
    else:
        sel = input('\nEnter paper number to query (or exact title): ').strip()
        if sel.isdigit():
            idxn = int(sel) - 1
            if idxn < 0 or idxn >= len(papers):
                print('Index out of range')
                return
            paper_title = papers[idxn].get(title_key)
        else:
            paper_title = sel

    if args.question:
        pregunta = args.question
    else:
        pregunta = input('Type your question about the paper: ').strip()

    gem_key = os.getenv('GEMINI_API_KEY')
    use_gemini_env = os.getenv('USE_GEMINI', '0') == '1'
    if gem_key:
        print('\n(GEMINI_API_KEY found in environment — value is NOT shown here)')
    else:
        print('\n(GEMINI_API_KEY not found — will use local mode to answer)')
    if use_gemini_env:
        print('(USE_GEMINI is set to 1 — attempting to use Gemini if key is present)')

    respuesta = ChatbotEngine.answer(paper_title, pregunta, papers)
    print('\n--- RESPONSE ---\n')
    print(respuesta)


if __name__ == '__main__':
    main()
