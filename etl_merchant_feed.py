#!/usr/bin/env python3
"""
ETL: Gomag feed (marsupiu.ro) -> Google Merchant Center XML valid
"""

import os
import re
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from xml.etree import ElementTree as ET
from lxml import etree
import requests

# ── Config ───────────────────────────────────────────────────────────────────
GOMAG_FEED_URL  = "https://www.marsupiu.ro/feed/merchant.xml"
OUTPUT_FILE     = "merchant_feed.xml"
STORE_NAME      = "Marsupiu.ro"
STORE_URL       = "https://www.marsupiu.ro"

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
EMAIL_USER       = os.environ.get("EMAIL_USER", "")
EMAIL_PASS       = os.environ.get("EMAIL_PASS", "")
EMAIL_TO         = os.environ.get("EMAIL_TO", "ghineabogdan.macara@gmail.com")

G_NS = "http://base.google.com/ns/1.0"

GOOGLE_CATEGORY = {
    "marsupiu": "Baby & Toddler > Baby Transport > Baby Carriers",
    "ham":      "Baby & Toddler > Baby Transport > Baby Reins & Harnesses",
}

# ── Helpers ──────────────────────────────────────────────────────────────────
def get_text(item, *tags):
    """Incearca mai multe tag-uri, returneaza primul cu valoare."""
    for tag in tags:
        el = item.find(tag)
        if el is not None and el.text:
            return el.text.strip()
    return ""

def fix_price(val):
    """'159' sau '189 Lei' -> '159.00 RON'"""
    if not val:
        return None
    num = re.sub(r"[^\d,.]", "", val).replace(",", ".")
    try:
        return f"{float(num):.2f} RON"
    except (ValueError, TypeError):
        return None

def fix_availability(val):
    v = (val or "").strip().lower()
    return "in stock" if v in ["da", "yes", "1", "true", "in stock"] else "out of stock"

def detect_type(title):
    t = title.lower()
    if "marsupiu" in t:
        return "marsupiu"
    if "ham" in t:
        return "ham"
    return ""

# ── Notificari ───────────────────────────────────────────────────────────────
def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": message},
            timeout=10,
        )
    except Exception:
        pass

def send_email(subject, body):
    if not EMAIL_USER or not EMAIL_PASS:
        return
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_USER
        msg["To"]      = EMAIL_TO
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_USER, EMAIL_PASS)
            s.sendmail(EMAIL_USER, EMAIL_TO, msg.as_string())
    except Exception:
        pass

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    errors    = []
    processed = 0
    skipped   = 0
    now       = datetime.now().strftime("%d.%m.%Y %H:%M")

    # 1. Descarca feed Gomag
    try:
        resp = requests.get(GOMAG_FEED_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        msg = f"❌ Merchant Feed ETL — {now}\nNu pot descarca feed-ul Gomag:\n{e}"
        send_telegram(msg)
        send_email("❌ EROARE Merchant Feed ETL", msg)
        raise SystemExit(1)

    # 2. Parseaza XML input (repara <> generat de Gomag daca Tag Produs e gol)
    try:
        xml_text = resp.text
        xml_text = xml_text.replace("<>", "<item>").replace("</>", "</item>")
        root = ET.fromstring(xml_text.encode("utf-8"))
    except Exception as e:
        msg = f"❌ Merchant Feed ETL — {now}\nXML invalid de la Gomag:\n{e}"
        send_telegram(msg)
        send_email("❌ EROARE Merchant Feed ETL", msg)
        raise SystemExit(1)

    channel = root.find("channel")
    items   = channel.findall("item") if channel is not None else []

    # 3. Construieste XML output cu namespace Google
    nsmap = {"g": G_NS}
    rss   = etree.Element("rss", version="2.0", nsmap=nsmap)
    ch    = etree.SubElement(rss, "channel")
    etree.SubElement(ch, "title").text       = STORE_NAME
    etree.SubElement(ch, "link").text        = STORE_URL
    etree.SubElement(ch, "description").text = f"Feed produse {STORE_NAME}"

    G = f"{{{G_NS}}}"

    for item in items:
        pid          = get_text(item, "id")
        title        = get_text(item, "title")
        description  = get_text(item, "description", "descriere-fara-html")
        link         = get_text(item, "link")
        image_link   = get_text(item, "image_link")
        sale_price_r = get_text(item, "sale_price")
        regular_pr   = get_text(item, "regular_price")
        availability = get_text(item, "availability")
        brand        = get_text(item, "brand") or "Cute4Babies"
        gtin         = get_text(item, "gtin")

        # Validare campuri obligatorii
        missing = [f for f, v in [("id", pid), ("title", title), ("link", link), ("image_link", image_link)] if not v]
        if missing:
            skipped += 1
            errors.append(f"Produs {pid or '?'}: lipsesc {', '.join(missing)}")
            continue

        # Pretul principal (regular_price sau sale_price ca fallback)
        price = fix_price(regular_pr) or fix_price(sale_price_r)
        if not price:
            skipped += 1
            errors.append(f"Produs {pid} ({title[:30]}): pret invalid")
            continue

        sale_price = fix_price(sale_price_r)

        product_type = detect_type(title)

        entry = etree.SubElement(ch, "item")
        etree.SubElement(entry, f"{G}id").text           = pid
        etree.SubElement(entry, f"{G}title").text        = title
        etree.SubElement(entry, f"{G}description").text  = description
        etree.SubElement(entry, f"{G}link").text         = link
        etree.SubElement(entry, f"{G}image_link").text   = image_link
        etree.SubElement(entry, f"{G}price").text        = price

        if sale_price and sale_price != price:
            etree.SubElement(entry, f"{G}sale_price").text = sale_price

        etree.SubElement(entry, f"{G}availability").text = fix_availability(availability)
        etree.SubElement(entry, f"{G}condition").text    = "new"
        etree.SubElement(entry, f"{G}brand").text        = brand

        if gtin:
            etree.SubElement(entry, f"{G}gtin").text = gtin
        else:
            etree.SubElement(entry, f"{G}identifier_exists").text = "no"

        google_cat = GOOGLE_CATEGORY.get(product_type, "Baby & Toddler > Baby Transport")
        etree.SubElement(entry, f"{G}google_product_category").text = google_cat

        if product_type:
            etree.SubElement(entry, f"{G}custom_label_0").text = product_type

        if product_type == "ham":
            etree.SubElement(entry, f"{G}item_group_id").text = "c4b_ham"

        processed += 1

    # 4. Scrie fisier output
    tree = etree.ElementTree(rss)
    with open(OUTPUT_FILE, "wb") as f:
        tree.write(f, xml_declaration=True, encoding="UTF-8", pretty_print=True)

    # 5. Notificari
    status = f"✅ Merchant Feed ETL\n{now}\nProduse procesate: {processed}"
    if skipped:
        status += f"\nSarite: {skipped}"
    if errors:
        status += f"\n\n⚠️ Avertismente:\n" + "\n".join(errors[:5])
        send_email("⚠️ Avertismente Merchant Feed ETL", "\n".join(errors))

    send_telegram(status)
    print(status.encode("ascii", errors="replace").decode("ascii"))

if __name__ == "__main__":
    main()
