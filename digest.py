#!/usr/bin/env python3
"""Персональный фильтр Telegram-каналов.

Забирает посты за последние сутки, отсеивает рекламу и дубли, отправляет всё
одним запросом в DeepSeek для ранжирования по личной значимости и присылает
короткий дайджест в Избранное (Saved Messages).

Первый запуск (интерактивный вход в Telegram):  python digest.py --dry-run
Боевой запуск по крону:                         python digest.py
"""

import argparse
import asyncio
import hashlib
import html
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv
from openai import OpenAI
from telethon import TelegramClient

if sys.platform == "win32":  # стабильный event loop для asyncio на Windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

BASE_DIR = Path(__file__).resolve().parent
MSK = timezone(timedelta(hours=3))
TG_MSG_LIMIT = 4096            # лимит Telegram на длину одного сообщения
CONTEXT_CHAR_BUDGET = 120_000  # защита от переполнения контекста модели

PROMPT_TEMPLATE = """Ты персональный фильтр новостей для одного конкретного человека.

ПРОФИЛЬ ЧЕЛОВЕКА:
{PROFILE}

КРИТЕРИЙ ЗНАЧИМОСТИ: тема важна, только если она способна повлиять на решения,
деньги, работу или семью этого человека в ближайшие 1-3 месяца. "Громкое" или
"интересное" событие без личного эффекта получает низкий балл.

ЗАДАЧА:
1. Сгруппируй посты об одном и том же событии в одну тему: одна новость часто
   повторяется в разных каналах.
2. Каждой теме поставь score от 0 до 10 по силе влияния именно на этого человека.
   Будь скуп: 8-10 это редкость (прямое влияние на деньги или планы), 6-7 значимо,
   4-5 фон, 0-3 шум.
3. Для тем со score 6 и выше заполни why (почему это влияет лично на него,
   1-2 предложения) и action (конкретный следующий шаг, если он есть, иначе null).
4. Ничего не выдумывай, опирайся только на факты из постов.

Ответь строго валидным json без пояснений, схема:
{"topics": [{"title": "суть темы одной строкой, до 90 знаков, без кликбейта",
"score": 7, "why": "строка или null", "action": "строка или null",
"post_ids": [3, 17]}]}"""


def load_config():
    with open(BASE_DIR / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def esc(s):
    return html.escape(str(s)) if s else ""


def fmt_n(n, one, few, many):
    """1 пост, 2 поста, 5 постов."""
    m = abs(n) % 100
    d = m % 10
    word = many if 11 <= m <= 14 else one if d == 1 else few if 2 <= d <= 4 else many
    return f"{n} {word}"


def normalize(text):
    """Нормализация текста для поиска дословных репостов."""
    return re.sub(r"\W+", "", text.lower())[:400]


def compile_ad_patterns(cfg):
    return [re.compile(p, re.IGNORECASE) for p in cfg.get("ad_patterns", [])]


async def fetch_posts(client, cfg):
    """Собирает посты за hours_lookback часов из всех каналов конфига."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cfg["hours_lookback"])
    patterns = compile_ad_patterns(cfg)
    posts, warnings = [], []
    seen_hashes, seen_groups = set(), set()

    for ch in cfg["channels"]:
        try:
            entity = await client.get_entity(ch)
        except Exception as e:
            warnings.append(f"канал {ch}: {e}")
            continue
        channel_name = getattr(entity, "title", None) or str(ch)
        username = getattr(entity, "username", None)

        async for msg in client.iter_messages(entity, limit=500):
            if msg.date < cutoff:
                break
            text = (msg.raw_text or "").strip()
            if msg.grouped_id:  # альбом из нескольких фото: берём только подпись
                if not text or msg.grouped_id in seen_groups:
                    continue
                seen_groups.add(msg.grouped_id)
            if not text or any(p.search(text) for p in patterns):
                continue
            h = hashlib.md5(normalize(text).encode()).hexdigest()
            if h in seen_hashes:  # дословный репост из другого канала
                continue
            seen_hashes.add(h)
            link = (f"https://t.me/{username}/{msg.id}" if username
                    else f"https://t.me/c/{entity.id}/{msg.id}")
            posts.append({
                "channel": channel_name,
                "link": link,
                "_dt": msg.date,
                "time": msg.date.astimezone(MSK).strftime("%d.%m %H:%M"),
                "text": text[: cfg["max_post_chars"]],
            })

    posts.sort(key=lambda p: p["_dt"])

    # Защита от переполнения контекста: если постов слишком много, режем старые
    total = 0
    kept = []
    for p in reversed(posts):
        total += len(p["text"])
        if total > CONTEXT_CHAR_BUDGET:
            break
        kept.append(p)
    dropped = len(posts) - len(kept)
    if dropped > 0:
        warnings.append(f"отброшено {dropped} старых постов из-за лимита контекста")
    posts = list(reversed(kept))

    for i, p in enumerate(posts, 1):
        p["idx"] = i
    return posts, warnings


def rank_topics(posts, cfg):
    """Один запрос к DeepSeek: дедупликация тем и оценка личной значимости."""
    llm = OpenAI(api_key=os.environ["DEEPSEEK_API_KEY"],
                 base_url="https://api.deepseek.com")
    system = PROMPT_TEMPLATE.replace("{PROFILE}", cfg["profile"].strip())
    user_msg = "\n\n".join(
        f"[{p['idx']}] ({p['channel']}, {p['time']})\n{p['text']}" for p in posts
    )
    resp = llm.chat.completions.create(
        model=cfg.get("model", "deepseek-chat"),
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user_msg}],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=4000,
    )
    data = json.loads(resp.choices[0].message.content)
    topics = data.get("topics", [])
    for t in topics:
        try:
            t["score"] = int(float(t.get("score") or 0))
        except (TypeError, ValueError):
            t["score"] = 0
        t["post_ids"] = [i for i in t.get("post_ids", []) if isinstance(i, int)]
    topics.sort(key=lambda t: -t["score"])
    return topics


def links_html(topic, by_idx, limit=2):
    out = []
    for pid in topic["post_ids"][:limit]:
        p = by_idx.get(pid)
        if p:
            out.append(f'<a href="{p["link"]}">{esc(p["channel"])}</a>')
    return " · ".join(out)


def build_digest(topics, posts, cfg, warnings):
    by_idx = {p["idx"]: p for p in posts}
    main = topics[0] if topics and topics[0]["score"] >= cfg["main_threshold"] else None
    rest = topics[1:] if main else topics
    worth = [t for t in rest if t["score"] >= cfg["worth_threshold"]][: cfg["max_worth_items"]]
    worth_ids = {id(t) for t in worth}
    rejected = [t for t in rest if id(t) not in worth_ids]

    today = datetime.now(MSK).strftime("%d.%m.%Y")
    n_channels = len({p["channel"] for p in posts})
    header_stats = (f"{fmt_n(len(posts), 'пост', 'поста', 'постов')} из "
                    f"{fmt_n(n_channels, 'канала', 'каналов', 'каналов')}")
    lines = [f"<b>Дайджест {today}</b> · {header_stats}"]

    if main:
        lines += ["", f"<b>Главное: {esc(main['title'])}</b>"]
        if main.get("why"):
            lines.append(esc(main["why"]))
        if main.get("action"):
            lines.append(f"Что сделать: {esc(main['action'])}")
        l = links_html(main, by_idx)
        if l:
            lines.append(l)
    else:
        lines += ["", "Сегодня ничего, что требовало бы вашего внимания."]

    if worth:
        lines += ["", "<b>Стоит знать:</b>"]
        for t in worth:
            l = links_html(t, by_idx, limit=1)
            lines.append(f"• {esc(t['title'])}" + (f" · {l}" if l else ""))

    if rejected:
        skipped_posts = sum(len(t["post_ids"]) for t in rejected)
        lines += ["", f"Остальное ({fmt_n(len(rejected), 'тема', 'темы', 'тем')}, "
                      f"{fmt_n(skipped_posts, 'пост', 'поста', 'постов')}) "
                      f"не стоит вашего времени."]

    if cfg.get("calibration") and rejected:
        lines += ["", "<i>Отфильтровано (режим калибровки):</i>"]
        for t in rejected:
            lines.append(f"<i>• [{t['score']}] {esc(t['title'])}</i>")

    if warnings:
        lines += ["", f"<i>Проблемы: {esc('; '.join(warnings))}</i>"]

    return "\n".join(lines)


def split_message(text, limit=TG_MSG_LIMIT):
    parts, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:  # аварийный случай: одна строка длиннее лимита
            parts.append(line[:limit])
            line = line[limit:]
        if len(cur) + len(line) + 1 > limit:
            parts.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        parts.append(cur)
    return parts


async def main():
    ap = argparse.ArgumentParser(description="Персональный дайджест Telegram-каналов")
    ap.add_argument("--dry-run", action="store_true",
                    help="напечатать дайджест в консоль, не отправлять в Telegram")
    args = ap.parse_args()

    load_dotenv(BASE_DIR / ".env")
    for var in ("TG_API_ID", "TG_API_HASH", "DEEPSEEK_API_KEY"):
        if not os.environ.get(var):
            sys.exit(f"Не задана переменная {var}, см. env.example")
    cfg = load_config()

    client = TelegramClient(str(BASE_DIR / "digest_session"),
                            int(os.environ["TG_API_ID"]), os.environ["TG_API_HASH"])
    await client.start()

    try:
        # прогрев кэша сущностей, без этого приватные каналы по ID не резолвятся
        await client.get_dialogs()
        posts, warnings = await fetch_posts(client, cfg)
        if not posts:
            digest = "За сутки ни одного содержательного поста (после фильтра рекламы)."
            if warnings:
                digest += "\nПроблемы: " + esc("; ".join(warnings))
        else:
            topics = rank_topics(posts, cfg)
            digest = build_digest(topics, posts, cfg, warnings)
    except SystemExit:
        raise
    except Exception as e:
        digest = f"Дайджест упал с ошибкой: {esc(repr(e))}"
        if not args.dry_run:
            # ошибки всегда в Избранное, даже если дайджест идёт в канал
            for part in split_message(digest):
                await client.send_message("me", part, parse_mode="html",
                                          link_preview=False)
        await client.disconnect()
        raise

    if args.dry_run:
        print(re.sub(r"</?[a-z][^>]*>", "", digest))
    else:
        target = cfg.get("target", "me")
        for part in split_message(digest):
            await client.send_message(target, part, parse_mode="html",
                                      link_preview=False)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())