"""
Telegram Command Listener
Polls for /scan commands and triggers the GitHub Actions scanner workflow.
Runs every 5 minutes via its own GitHub Actions workflow.
"""

import os, json, requests, time
from pathlib import Path
from datetime import datetime, timezone

# ── CONFIG ───────────────────────────────────────────────────
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID   = str(os.environ["TG_CHAT_ID"]).strip()
GH_PAT       = os.environ["GH_PAT"]          # Personal Access Token (workflow scope)
GH_REPO      = os.environ["GH_REPO"]         # e.g. "yourname/yourrepo"
GH_BRANCH    = os.environ.get("GH_BRANCH", "main")

OFFSET_FILE  = "tg_offset.json"
SCANNER_WF   = "scanner.yml"


# ═══════════════════════════════════════════════════════════════
# OFFSET STATE  (avoids re-processing old /scan commands)
# ═══════════════════════════════════════════════════════════════

def load_offset() -> int | None:
    if Path(OFFSET_FILE).exists():
        try:
            data = json.loads(Path(OFFSET_FILE).read_text())
            return data.get("offset")
        except Exception:
            pass
    return None


def save_offset(offset: int):
    Path(OFFSET_FILE).write_text(json.dumps({"offset": offset}, indent=2))


# ═══════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════════════

def get_updates(offset: int | None) -> list[dict]:
    """
    Fetch new Telegram updates since `offset`.
    Returns both regular messages AND channel posts.
    """
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getUpdates"
    params: dict = {
        "timeout":          5,
        "allowed_updates":  ["message", "channel_post"],
    }
    if offset is not None:
        params["offset"] = offset

    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            if data.get("ok"):
                return data.get("result", [])
        except Exception as e:
            if attempt == 2:
                print(f"[TG getUpdates ERROR] {e}")
            time.sleep(2 ** attempt)
    return []


def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    for attempt in range(3):
        try:
            r = requests.post(url, json={
                "chat_id":    TG_CHAT_ID,
                "text":       text,
                "parse_mode": "HTML",
            }, timeout=10)
            r.raise_for_status()
            return
        except Exception as e:
            if attempt == 2:
                print(f"[TG send ERROR] {e}")
            time.sleep(2)


# ═══════════════════════════════════════════════════════════════
# GITHUB ACTIONS TRIGGER
# ═══════════════════════════════════════════════════════════════

def trigger_scanner() -> bool:
    """
    Dispatch the scanner workflow via the GitHub REST API.
    Requires a PAT with 'workflow' scope stored as GH_PAT secret.
    """
    url = (
        f"https://api.github.com/repos/{GH_REPO}"
        f"/actions/workflows/{SCANNER_WF}/dispatches"
    )
    headers = {
        "Authorization": f"token {GH_PAT}",
        "Accept":        "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload = {"ref": GH_BRANCH}

    for attempt in range(3):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code == 204:
                return True
            print(f"[GH API] status={r.status_code}  body={r.text[:200]}")
        except Exception as e:
            if attempt == 2:
                print(f"[GH API ERROR] {e}")
        time.sleep(2 ** attempt)
    return False


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def is_scan_command(text: str) -> bool:
    """Match /scan or /scan@BotName (Telegram appends bot name in groups/channels)."""
    t = text.strip().lower().split("@")[0]
    return t == "/scan"


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"[{ts}] Telegram listener starting…")

    offset = load_offset()
    print(f"  Current update offset: {offset}")

    updates = get_updates(offset)
    print(f"  Fetched {len(updates)} update(s)")

    scan_requested  = False
    new_offset      = offset

    for update in updates:
        update_id = update.get("update_id", 0)
        new_offset = update_id + 1   # advance offset past this update

        # Handle both normal messages (group/DM) and channel posts
        msg = update.get("message") or update.get("channel_post") or {}
        text    = (msg.get("text") or "").strip()
        chat_id = str(msg.get("chat", {}).get("id", ""))

        print(f"  Update {update_id}: chat={chat_id}  text={text!r}")

        # Security: only accept commands from the configured chat
        if chat_id != TG_CHAT_ID:
            print(f"    Ignored (chat {chat_id} != configured {TG_CHAT_ID})")
            continue

        if is_scan_command(text):
            scan_requested = True
            print("    /scan command detected!")

    # Persist new offset so next run doesn't re-process these updates
    if new_offset and new_offset != offset:
        save_offset(new_offset)
        print(f"  Offset saved: {new_offset}")

    # Trigger scanner if requested
    if scan_requested:
        send_telegram(
            "🔍 <b>Manual scan triggered!</b>\n"
            "Running the signal scanner now — results will follow shortly."
        )
        success = trigger_scanner()
        if success:
            print("  ✅ Scanner workflow dispatched successfully.")
        else:
            send_telegram(
                "⚠️ <b>Failed to trigger scanner.</b>\n"
                "Check GitHub Actions permissions or PAT token."
            )
            print("  ❌ Failed to dispatch scanner workflow.")
    else:
        print("  No /scan command found. Nothing to do.")

    print("Listener run complete.")


if __name__ == "__main__":
    main()
