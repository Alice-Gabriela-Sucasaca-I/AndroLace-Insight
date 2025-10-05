import os, ssl, re
import requests
import pymysql

from lxml import etree

from dotenv import load_dotenv, find_dotenv
dotenv_path = find_dotenv(filename=".env", usecwd=True)
load_dotenv(dotenv_path=dotenv_path, override=True)
print("[ENV] loaded from:", dotenv_path)
import os
print("[ENV] MYSQL_HOST=", os.getenv("MYSQL_HOST"))
print("[ENV] MYSQL_USER=", os.getenv("MYSQL_USER"))
print("[ENV] MYSQL_DB=", os.getenv("MYSQL_DB"))


def get_conn():
    ssl_ca = os.getenv("MYSQL_SSL_CA")
    ssl_ctx = None
    if ssl_ca and os.path.exists(ssl_ca):
        ssl_ctx = ssl.create_default_context(cafile=ssl_ca)
    
    return pymysql.connect(
        host=os.getenv("nasa-bio-nasa-bio.e.aivencloud.com"),
        port=int(os.getenv("AIVEN_MYSQL_PORT", "21245")),
        user=os.getenv("AIVEN_MYSQL_USER", "avnadmin"),  
        password=os.getenv("AVNS_ZBq0-je5rrtFAJLwUIL"),  
        database=os.getenv("AIVEN_MYSQL_DATABASE", "bio_papers_db"),  
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        ssl=ssl_ctx,
        connect_timeout=20,
    )

def clean_text(s):
    if not s: return None
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def to_int(s):
    try:
        return int(s)
    except:
        return None

def fetch_jats_xml(pmcid: str) -> bytes:
    num = pmcid.replace("PMC","")
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {"db": "pmc", "id": num, "retmode": "xml"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.content


def parse_article_title(jats_xml: bytes):
    root = etree.fromstring(jats_xml)
    title_el = root.find(".//article-title")
    title = clean_text("".join(title_el.itertext())) if title_el is not None else None
    return title


#parse references: extractor
def parse_references(jats_xml: bytes):
    root = etree.fromstring(jats_xml)
    refs = root.xpath(".//ref-list//ref")
    items = []
    for ref in refs:
        cit = ref.find(".//element-citation")
        if cit is None:
            cit = ref.find(".//mixed-citation")
        if cit is None:
            continue
        title_el = cit.find(".//article-title")
        title = clean_text("".join(title_el.itertext())) if title_el is not None else None
        journal_el = cit.find(".//source")
        journal = clean_text(journal_el.text) if journal_el is not None else None
        year_el = cit.find(".//year")
        year = to_int(year_el.text) if year_el is not None else None
        names = []
        for name in cit.findall(".//name"):
            surname = clean_text(name.findtext("surname"))
            given = clean_text(name.findtext("given-names"))
            if surname and given: names.append(f"{surname}, {given}")
            elif surname: names.append(surname)
        authors = "; ".join(names) if names else None

        doi = pmid = pmcid = None
        for pid in cit.findall(".//pub-id"):
            pid_type = (pid.get("pub-id-type") or "").lower()
            val = clean_text(pid.text)
            if pid_type == "doi":
                doi = val
            elif pid_type == "pmid":
                pmid = val
            elif pid_type in ("pmcid","pmc"):
                pmcid = val if val.startswith("PMC") else f"PMC{val}"
        if not doi:
            for x in cit.findall(".//ext-link"):
                if (x.get("ext-link-type") or "").lower() == "doi":
                    val = clean_text("".join(x.itertext()))
                    val = val.replace("https://doi.org/","").replace("http://doi.org/","").replace("doi:","")
                    doi = val
                    break
        items.append({
            "title": title, "journal": journal, "year": year,
            "doi": doi, "pmid": pmid, "pmcid": pmcid, "authors": authors
        })
    print(f"[PARSE] Referencias encontradas: {len(items)}")
    return items

def upsert_paper(cur, rec):
    if rec.get("doi"):
        cur.execute("SELECT id_paper FROM paper WHERE doi=%s", (rec["doi"],))
        row = cur.fetchone()
        if row:
            return row["id_paper"]
    cur.execute("""
        INSERT INTO paper (title, abstract, year, journal, doi)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            title=VALUES(title),
            year=VALUES(year),
            journal=VALUES(journal)
    """, (rec.get("title"), None, rec.get("year"), rec.get("journal"), rec.get("doi")))
    if rec.get("doi"):
        cur.execute("SELECT id_paper FROM paper WHERE doi=%s", (rec["doi"],))
        return cur.fetchone()["id_paper"]
    else:
        cur.execute("SELECT id_paper FROM paper WHERE title=%s AND (year <=> %s)", (rec.get("title"), rec.get("year")))
        r = cur.fetchone()
        return r["id_paper"] if r else None

def ingest_references_for_pmcid(pmcid: str):
    xml = fetch_jats_xml(pmcid)
    main_title = parse_article_title(xml) or f"PMC {pmcid}"
    refs = parse_references(xml)

    with get_conn() as conn, conn.cursor() as cur:
        main_doi = f"pmcid:{pmcid}"
        cur.execute("""
            INSERT INTO paper (title, abstract, year, journal, doi)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE title=VALUES(title)
        """, (main_title, None, None, None, main_doi))
        cur.execute("SELECT id_paper FROM paper WHERE doi=%s", (main_doi,))
        main_id = cur.fetchone()["id_paper"]
        print(f"[MAIN] id_paper={main_id} | title='{main_title}'")

        created = 0
        for i, r in enumerate(refs, start=1):
            cited_id = upsert_paper(cur, r)
            if cited_id:
                cur.execute("""
                    INSERT INTO citation (id_paper_used, id_paper_used_by)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE id_paper_used=id_paper_used
                """, (cited_id, main_id))
                created += 1
            if i % 10 == 0:
                print(f"  - procesadas {i}/{len(refs)}")

        conn.commit()
        print(f"[DONE] refs_parsed={len(refs)} | edges_created={created} | main_id={main_id}")
        return {"main_id": main_id, "refs_parsed": len(refs), "edges_created": created}

if __name__ == "__main__":
    result = ingest_references_for_pmcid("PMC4136787")
    print(result)
