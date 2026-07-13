#!/usr/bin/env python3
"""
Raspunde la /help sau /info pe Telegram cu lista de comenzi disponibile.
Ruleaza periodic (la fiecare ~15 min) prin GitHub Actions. Nu tine stare
locala - foloseste mecanismul de "offset" al Telegram ca sa nu proceseze
de doua ori acelasi mesaj.
"""

import os
import requests

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HELP_TEXT = (
    "Acest bot (Merchant Feed ETL) trimite automat, zilnic la 08:00, "
    "rezultatul ETL-ului Gomag -> Google Merchant Center (produse "
    "procesate, erori daca apar).\n\n"
    "Nu are comenzi interactive momentan - doar notificari automate."
)


def get_updates():
    resp = requests.get(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
        params={"timeout": 0},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("result", [])


def confirm_updates(max_update_id: int):
    requests.get(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
        params={"offset": max_update_id + 1, "timeout": 0},
        timeout=15,
    )


def send_telegram(message: str):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data={"chat_id": TELEGRAM_CHAT_ID, "text": message},
        timeout=10,
    )


def main():
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID):
        print("Telegram nu e configurat.")
        return

    updates = get_updates()
    if not updates:
        print("Niciun mesaj nou.")
        return

    max_id = max(u["update_id"] for u in updates)
    has_message = any(u.get("message", {}).get("text", "").strip() for u in updates)

    if has_message:
        send_telegram(HELP_TEXT)

    confirm_updates(max_id)


if __name__ == "__main__":
    main()
