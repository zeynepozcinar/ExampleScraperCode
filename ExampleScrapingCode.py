#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
from typing import List, Dict, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse, parse_qs

# ---------- SITE CONSTANTS ----------
BASE = "https://www.examplesite.com"
LIST_URL = f"{BASE}/en/collections/all"
OUT_XLSX = "example.xlsx"

TIMEOUT = 20
RETRIES = 3
SLEEP_BETWEEN_SERIES = 0.2
SLEEP_BETWEEN_PRODUCTS = 0.1
ITEM_LIMIT = 99999  # increase or set to None for full crawl

# ---------- REQUESTS HELPERS ----------
def fetch(url: str) -> Optional[str]:
    headers = {
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/123 Safari/537.36")
    }
    for _ in range(RETRIES):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            time.sleep(0.6)
        except requests.RequestException:
            time.sleep(0.6)
    print(f" Failed to load: {url}")
    return None

def soup_from(url: str) -> Optional[BeautifulSoup]:
    html = fetch(url)
    if not html:
        return None
    return BeautifulSoup(html, "html.parser")

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def absolutize(href: str, base: str = BASE) -> str:
    if not href:
        return ""
    return urljoin(base, href)

def slugify(text: str) -> str:
    t = (text or "").lower().replace("×", "x")
    t = t.replace("ç","c").replace("ğ","g").replace("ı","i").replace("ö","o").replace("ş","s").replace("ü","u")
    t = re.sub(r"[^a-z0-9\s_\-\.x]", "", t)
    t = "_".join(t.split())
    return t

# ---------- IMAGE URL UNWRAPPER ----------
def _unwrap_proxied(src: str) -> str:
    if not src:
        return ""
    if src.startswith("//"):
        src = "https:" + src
    if src.startswith("/"):
        src = BASE + src
    try:
        if "api/image" in src and "url=" in src:
            q = parse_qs(urlparse(src).query)
            if q.get("url") and q["url"][0]:
                return q["url"][0].strip()
    except Exception:
        pass
    return src

# ---------- SELENIUM (SHARED) ----------
_tex_driver = None
_tex_wait = None

def _ensure_tex_driver():
    """Create a single headless Selenium driver."""
    global _tex_driver, _tex_wait
    if _tex_driver is not None:
        return
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from webdriver_manager.chrome import ChromeDriverManager

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,1600")
    opts.add_argument("--lang=en-US")
    service = Service(ChromeDriverManager().install())
    d = webdriver.Chrome(service=service, options=opts)
    _tex_driver = d
    _tex_wait = WebDriverWait(d, 15)

def _selenium_load_and_accept(url: str):
    """Load a URL and accept cookies if present."""
    from selenium.webdriver.common.by import By
    _ensure_tex_driver()
    d = _tex_driver
    d.get(url)
    time.sleep(0.8)
    # accept cookies if present
    for sel in [
        "button#onetrust-accept-btn-handler",
        "button[aria-label*='Accept']",
        "button.cookie-accept",
    ]:
        try:
            btns = d.find_elements(By.CSS_SELECTOR, sel)
            if btns and btns[0].is_displayed():
                d.execute_script("arguments[0].click();", btns[0])
                time.sleep(0.2)
                break
        except Exception:
            pass
    return d

# ---------- TEXTURES (BS4 + SELENIUM) ----------
def extract_textures_bs4(sp: BeautifulSoup) -> str:
    urls, seen = [], set()
    sec = sp.select_one("section.ExampleSection")
    if not sec:
        return ""
    container = sec.select_one("div.ExampleContainer")
    if not container:
        return ""
    nodes = container.select("img.picture__image, source[srcset]")
    for node in nodes:
        src = (node.get("src") or node.get("data-src") or "").strip()
        if not src and node.name == "source":
            srcset = (node.get("srcset") or "").split(",")[0].strip()
            if srcset:
                src = srcset.split()[0]
        real = _unwrap_proxied(src)
        if real and real not in seen:
            seen.add(real)
            urls.append(real)
    return " | ".join(urls)

def extract_textures_selenium(product_url: str) -> str:
    """Open product, open Texture accordion, force-load all slides, collect URLs."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC

    try:
        d = _selenium_load_and_accept(product_url)
        wait = _tex_wait

        # scroll near details/texture area
        for y in (300, 900, 1400):
            d.execute_script(f"window.scrollTo(0,{y});")
            time.sleep(0.2)

        # find texture section & click the head button if present
        try:
            sec = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "section.ExampleSection")
            ))
            try:
                btn = sec.find_element(By.CSS_SELECTOR, ".texture-ExampleButton")
                if btn.is_displayed():
                    d.execute_script("arguments[0].click();", btn)
                    time.sleep(0.3)
            except Exception:
                pass
        except Exception:
            return ""

        # wait for the container
        try:
            wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.ExampleContainer")
            ))
        except Exception:
            return ""

        # center & horizontally scroll container to trigger lazy loading
        d.execute_script("""
            const sec = document.querySelector('section.Section');
            if (sec) sec.scrollIntoView({behavior:'instant', block:'center'});
        """)
        time.sleep(0.2)
        d.execute_script("""
            const c = document.querySelector('div.ExampleContainer');
            if (c) {
                const step = Math.max(400, Math.floor(c.scrollWidth / 6));
                for (let x = 0; x <= c.scrollWidth + 50; x += step) {
                    c.scrollTo(x, 0);
                }
            }
        """)
        time.sleep(0.4)

        # collect image and source urls 
        srcs = d.execute_script("""
            const out = [];
            const c = document.querySelector('div.exampleContainer");
            if (!c) return out;
            const nodes = c.querySelectorAll('img.picture__image, source[srcset]');
            for (const node of nodes) {
                let s = (node.getAttribute('src') || node.getAttribute('data-src') || '').trim();
                if (!s && node.tagName.toLowerCase() === 'source') {
                    const first = (node.getAttribute('srcset') || '').split(',')[0].trim().split(/\s+/)[0];
                    if (first) s = first;
                }
                if (s) out.push(s);
                if (node.tagName.toLowerCase() === 'img') {
                    const cs = (node.currentSrc || '').trim();
                    if (cs) out.push(cs);
                }
            }
            return out;
        """)

        urls, seen = [], set()
        for src in srcs:
            real = _unwrap_proxied(src)
            if real and real not in seen:
                seen.add(real)
                urls.append(real)
        return " | ".join(urls)
    except Exception:
        return ""

# ---------- DETAILS (BS4 + robust Selenium JS) ----------
def extract_detail_block_bs4(sp: BeautifulSoup, titles_en_it: List[str], mod: Optional[str]) -> Optional[BeautifulSoup]:
    # by modifier
    if mod:
        node = sp.select_one(f".exampleProductDetails")
        if node:
            return node
    # by title text
    for itm in sp.select(".exampleProductDetailsItem"):
        h = itm.select_one("h4 .title__content")
        if not h:
            continue
        title = clean_text(h.get_text()).lower()
        if title in [t.lower() for t in titles_en_it]:
            return itm
    return None

def extract_list_items_text(block: BeautifulSoup) -> List[str]:
    out = []
    if not block:
        return out
    for li in block.select(".details__item__list li"):
        txt = clean_text(li.get_text())
        if txt:
            out.append(txt)
    return out

def extract_details_bs4(sp: BeautifulSoup) -> Dict[str, List[str]]:
    formats_block = extract_detail_block_bs4(sp, ["Formats", "Formati", "Formato"], "formati")
    finishing_block = extract_detail_block_bs4(sp, ["Finishing", "Finiture", "Finitura"], "finiture")
    thickness_block = extract_detail_block_bs4(sp, ["Thickness", "Spessori", "Spessore"], "spessori")
    characteristics_block = extract_detail_block_bs4(sp, ["Characteristics", "Caratteristiche"], "caratteristiche")

    return {
        "Formats": extract_list_items_text(formats_block),
        "Finishing": extract_list_items_text(finishing_block),
        "Thickness": extract_list_items_text(thickness_block),
        "Characteristics_text": " ".join([
            clean_text(p.get_text()) for p in (characteristics_block.select(".paragraph, p") if characteristics_block else [])
            if clean_text(p.get_text())
        ]) if characteristics_block else ""
    }

def extract_details_selenium(product_url: str) -> Dict[str, List[str]]:
    """
    Uses JS in the page to iterate each .details__item, read its title/modifier,
    and collect <li> texts + paragraphs for Characteristics.
    """
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    result = {"Formats": [], "Finishing": [], "Thickness": [], "Characteristics_text": ""}

    try:
        d = _selenium_load_and_accept(product_url)
        wait = _tex_wait

        # ensure details are rendered
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".exampleProductDetails")))
        except Exception:
            return result

        # small scroll nudges can trigger late mounts
        for y in (300, 800, 1200):
            d.execute_script(f"window.scrollTo(0,{y});")
            time.sleep(0.15)

        data = d.execute_script("""
            const res = {formats:[], finishing:[], thickness:[], characteristics_text:""};
            const root = document.querySelector('.product-hero__details');
            if (!root) return res;
            const blocks = root.querySelectorAll('.details__item');
            const norm = s => (s||"").trim().toLowerCase();
            for (const b of blocks) {
                const title = norm(b.querySelector('h4 .title__content')?.textContent);
                const cls = Array.from(b.classList).find(c => c.startsWith('details__item--')) || "";
                const mod = norm(cls.replace('details__item--',''));
                const items = Array.from(b.querySelectorAll('.details__item__list li'))
                                  .map(li => norm(li.textContent))
                                  .filter(Boolean);
                const paras = Array.from(b.querySelectorAll('.paragraph, p'))
                                  .map(p => norm(p.textContent))
                                  .filter(Boolean)
                                  .join(' ');
                const is = keys => keys.includes(title) || keys.includes(mod);
                if (is(['formats','formati','formato'])) res.formats = items;
                else if (is(['finishing','finiture','finitura'])) res.finishing = items;
                else if (is(['thickness','spessori','spessore'])) res.thickness = items;
                else if (is(['characteristics','caratteristiche'])) res.characteristics_text = paras;
            }
            return res;
        """)

        # convert back to display form (keep original punctuation like 6,5)
        result["Formats"] = [s.replace("  ", " ").strip() for s in data.get("formats", [])]
        result["Finishing"] = [s.replace("  ", " ").strip() for s in data.get("finishing", [])]
        result["Thickness"] = [s.replace("  ", " ").strip() for s in data.get("thickness", [])]
        result["Characteristics_text"] = (data.get("characteristics_text") or "").strip()
        return result
    except Exception:
        return result

# ---------- SCRAPER FLOW ----------
def extract_series_from_list() -> List[Tuple[str, str]]:
    sp = soup_from(LIST_URL)
    if not sp:
        return []
    series, seen = [], set()
    for a in sp.select("a.exampleSelectProductListItem"):
        href = absolutize(a.get("href", ""))
        ttl = a.select_one(".ItemTitle")
        name = clean_text(ttl.get_text()) if ttl else href.rstrip("/").split("/")[-1]
        key = (name.lower(), href)
        if key in seen:
            continue
        seen.add(key)
        if "/en/" not in href:
            parsed = urlparse(href)
            href = urljoin(BASE, "/en" + parsed.path)
        series.append((name, href))
    return series

def extract_product_cards(series_url: str) -> List[Tuple[str, str]]:
    sp = soup_from(series_url)
    if not sp:
        return []
    prods, seen = [], set()
    for a in sp.select("a.ProductItem"):
        href = absolutize(a.get("href", ""))
        ttl = a.select_one(".ProductItemContent")
        name = clean_text(ttl.get_text()) if ttl else href.rstrip("/").split("/")[-2]
        k = (name.lower(), href)
        if k in seen:
            continue
        seen.add(k)
        if "/en/" not in href:
            parsed = urlparse(href)
            href = urljoin(BASE, "/en" + parsed.path)
        prods.append((name, href))
    return prods

def parse_product_page(series_name: str, product_url: str) -> Dict:
    # force English path
    if "/en/" not in product_url:
        parsed = urlparse(product_url)
        product_url = urljoin(BASE, "/en" + parsed.path)

    sp = soup_from(product_url)
    if not sp:
        return {}

    # Product name
    name_node = sp.select_one(".exampleProductItemContent") or sp.select_one("h1 .title__content")
    prod_name = clean_text(name_node.get_text()) if name_node else product_url.rstrip("/").split("/")[-2]
    seri = clean_text(series_name)

    urun_kodu = slugify(f"{seri} {prod_name}")

    # ---- DETAILS: BS4 first, Selenium fallback if any important field missing
    det_bs4 = extract_details_bs4(sp)
    olculer_list = det_bs4.get("Formats", [])
    yuzey_list = det_bs4.get("Finishing", [])
    kalinlik_list = det_bs4.get("Thickness", [])
    urun_aciklamasi = det_bs4.get("Characteristics_text", "")

    if not (olculer_list and yuzey_list and kalinlik_list):
        det_sel = extract_details_selenium(product_url)
        if not olculer_list and det_sel.get("Formats"):
            olculer_list = det_sel["Formats"]
        if not yuzey_list and det_sel.get("Finishing"):
            yuzey_list = det_sel["Finishing"]
        if not kalinlik_list and det_sel.get("Thickness"):
            kalinlik_list = det_sel["Thickness"]
        if not urun_aciklamasi and det_sel.get("Characteristics_text"):
            urun_aciklamasi = det_sel["Characteristics_text"]

    # ---- ÜRÜN GÖRSELLER: BS4 then Selenium
    urun_gorseller = extract_textures_bs4(sp)
    if not urun_gorseller:
        urun_gorseller = extract_textures_selenium(product_url)

    return {
        "Seri": seri,
        "Ürün adı": prod_name,
        "Ürün Kodu": urun_kodu,
        "Ölçüler": ", ".join(olculer_list),
        "Yüzey": ", ".join(yuzey_list),
        "Kalınlık": ", ".join(kalinlik_list),
        "Ürün Görseller": urun_gorseller,
        "Ürün Açıklaması": urun_aciklamasi,
        "Ürün Linki": product_url,
    }

# ---------- MAIN ----------
def main():
    try:
        series = extract_series_from_list()
        if not series:
            print("No series found. Exiting.")
            return

        print(f"Found {len(series)} series on list page.")
        rows: List[Dict] = []
        total = 0

        for s_idx, (seri_name, seri_url) in enumerate(series, 1):
            if ITEM_LIMIT is not None and total >= ITEM_LIMIT:
                break
            print(f"[{s_idx}/{len(series)}] Series: {seri_name} -> {seri_url}")
            prods = extract_product_cards(seri_url)
            if not prods:
                continue
            for p_idx, (pname, purl) in enumerate(prods, 1):
                if ITEM_LIMIT is not None and total >= ITEM_LIMIT:
                    break
                print(f"   - Product {p_idx}: {pname}")
                row = parse_product_page(seri_name, purl)
                if row:
                    rows.append(row)
                    total += 1
                time.sleep(SLEEP_BETWEEN_PRODUCTS)
            time.sleep(SLEEP_BETWEEN_SERIES)

        if not rows:
            print("No products collected.")
            return

        df = pd.DataFrame(rows)
        df.drop_duplicates(subset=["Seri", "Ürün adı", "Ürün Linki"], inplace=True)

        cols = ["Seri", "Ürün adı", "Ürün Kodu", "Ölçüler", "Yüzey", "Kalınlık",
                "Ürün Görseller", "Ürün Açıklaması", "Ürün Linki"]
        df = df[[c for c in cols if c in df.columns]]

        df.to_excel(OUT_XLSX, index=False)
        print(f"Saved {len(df)} rows → {OUT_XLSX}")
    finally:
        global _tex_driver
        try:
            if _tex_driver is not None:
                _tex_driver.quit()
        except Exception:
            pass
        _tex_driver = None

if __name__ == "__main__":
    main()
