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
READ_SPEED = 1000              # знаков в минуту, консервативная скорость чтения

# Пожелание в конце дайджеста, ротация по дню года: без повторов подряд
WISHES = [
    "Хорошего дня. Главное вы уже знаете, остальное подождёт.",
    "Пусть день пройдёт по вашему плану, а не по новостному.",
    "Спокойного дня. Вы контролируете повестку, а не наоборот.",
    "Продуктивного дня. Ваше внимание при вас, тратьте его на важное.",
    "Хорошего дня. Решения принимают те, кто не тонет в шуме.",
    "Уверенного дня. Вы знаете достаточно, чтобы действовать.",
    "Пусть сегодня хватит времени на главное. Оно у вас теперь есть.",
    "Лёгкого дня. Информационный балласт остался за бортом.",
    "Хорошей работы сегодня. Фокус ваше конкурентное преимущество.",
    "Спокойствия и ясности. Всё важное уместилось в пару абзацев.",
    "Отличного дня. Сэкономленное время потратьте на себя.",
    "Вы быстрее ленты. Пусть день это подтвердит.",
    "Хорошего утра. Начинать день с сути правильная привычка.",
    "Уверенности в решениях. База для них у вас перед глазами.",
    "Пусть день будет содержательнее новостей.",
    "Ясной головы. Шум отфильтрован, курс ваш.",
]

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
   1-2 предложения).
4. Ничего не выдумывай, опирайся только на факты из постов.
5. Верни не больше 35 тем: одиночные малозначимые посты объединяй
   в сборные темы.

Ответь строго валидным json без пояснений, схема:
{"topics": [{"title": "суть темы одной строкой, до 90 знаков, без кликбейта",
"score": 7, "why": "строка или null", "post_ids": [3, 17]}]}"""

CHAT_PROMPT_TEMPLATE = """Тебе даны обсуждения из телеграм-чатов: номер, статистика
и выборочные сообщения каждой ветки. Для каждой ветки определи тему и суть.

Правила:
1. title: о чём спорили или говорили, до 60 знаков, без кликбейта.
2. gist: одно предложение с сутью или выводом обсуждения, если вывод был.
3. Ничего не выдумывай, только то, что видно в сообщениях.

Ответь строго валидным json:
{"threads": [{"id": 1, "title": "...", "gist": "..."}]}"""

WEATHER_PROMPT_TEMPLATE = """Ты метеодежурный. По постам погодных каналов за последние
сутки реши, нужно ли предупреждать жителя Москвы о завтрашней погоде.

Предупреждение нужно ТОЛЬКО если из постов явно следует хотя бы одно:
1. Завтра в Москве температура упадёт более чем на 10 градусов по сравнению
   с сегодняшним днём.
2. Завтра в Москве ожидается очень сильный дождь, ливень, град, шторм или
   объявлен жёлтый, оранжевый либо красный уровень погодной опасности.

Если явных указаний нет, речь не про Москву или не про завтра, предупреждение
не нужно. Любое сомнение трактуй как "не нужно". Не выдумывай числа.

Ответь строго валидным json:
{"alert": true или false, "message": "если alert true: 2-3 предложения, что именно
ожидается завтра с числами из постов и одна практическая рекомендация;
если false: пустая строка"}"""


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


async def fetch_posts(client, cfg, channels=None):
    """Собирает посты за hours_lookback часов. По умолчанию из cfg["channels"],
    но список можно переопределить (например, погодными каналами)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cfg["hours_lookback"])
    patterns = compile_ad_patterns(cfg)
    posts, warnings = [], []
    seen_hashes, seen_groups = set(), set()

    for ch in (channels if channels is not None else cfg["channels"]):
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
                "full_len": len(text),  # длина до обрезки, для расчёта экономии
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
        max_tokens=8000,
    )
    raw = resp.choices[0].message.content
    if resp.choices[0].finish_reason == "length":
        print("warning: ответ модели обрезан по лимиту токенов", file=sys.stderr)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # ответ оборвался: отрезаем хвост до последней целой темы,
        # закрываем JSON и работаем с тем, что успело прийти
        cut = raw.rfind("},")
        if cut == -1:
            raise
        data = json.loads(raw[: cut + 1] + "]}")
    topics = data.get("topics", [])
    for t in topics:
        try:
            t["score"] = int(float(t.get("score") or 0))
        except (TypeError, ValueError):
            t["score"] = 0
        t["post_ids"] = [i for i in t.get("post_ids", []) if isinstance(i, int)]
    topics.sort(key=lambda t: -t["score"])
    return topics


def llm_client():
    return OpenAI(api_key=os.environ["DEEPSEEK_API_KEY"],
                  base_url="https://api.deepseek.com")


def cluster_threads(msgs, min_size):
    """Собирает ветки обсуждений по цепочкам ответов (union-find).
    msgs: {id: {"id", "reply", "text", "re"}}. Возвращает списки сообщений."""
    parent = {mid: mid for mid in msgs}

    def find(x):
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for m in msgs.values():
        r = m["reply"]
        if r and r in msgs:
            ra, rb = find(m["id"]), find(r)
            if ra != rb:
                parent[ra] = rb

    clusters = {}
    for mid in msgs:
        clusters.setdefault(find(mid), []).append(msgs[mid])
    return [sorted(c, key=lambda m: m["id"])
            for c in clusters.values() if len(c) >= min_size]


async def fetch_chat_threads(client, cfg):
    """Вытаскивает из чатов ветки обсуждений, ранжирует по длине и реакциям."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=cfg["hours_lookback"])
    threads, warnings = [], []

    for ch in cfg.get("chats") or []:
        try:
            entity = await client.get_entity(ch)
        except Exception as e:
            warnings.append(f"чат {ch}: {e}")
            continue
        chat_name = getattr(entity, "title", None) or str(ch)
        username = getattr(entity, "username", None)

        msgs = {}
        async for m in client.iter_messages(entity, limit=2000):
            if m.date < cutoff:
                break
            reacts = 0
            if m.reactions and m.reactions.results:
                reacts = sum(r.count for r in m.reactions.results)
            msgs[m.id] = {"id": m.id, "reply": m.reply_to_msg_id,
                          "text": (m.raw_text or "").strip()[:250], "re": reacts}

        for items in cluster_threads(msgs, cfg.get("chat_min_thread", 4)):
            texted = [m for m in items if m["text"]]
            if not texted:
                continue
            top_reacted = sorted(texted, key=lambda m: -m["re"])[:4]
            picked = ({m["id"] for m in texted[:3]}
                      | {m["id"] for m in top_reacted}
                      | {m["id"] for m in texted[-2:]})
            sample = [m["text"] for m in items if m["id"] in picked][:8]
            reacts_total = sum(m["re"] for m in items)
            first_id = items[0]["id"]
            link = (f"https://t.me/{username}/{first_id}" if username
                    else f"https://t.me/c/{entity.id}/{first_id}")
            threads.append({"chat": chat_name, "link": link, "n": len(items),
                            "reacts": reacts_total, "sample": sample,
                            "rank": len(items) + reacts_total})

    threads.sort(key=lambda t: -t["rank"])
    return threads[: cfg.get("chat_topics_max", 5)], warnings


def name_chat_topics(threads, cfg):
    """Один вызов LLM: назвать тему и суть каждой ветки."""
    blocks = []
    for i, t in enumerate(threads, 1):
        sample = "\n".join(f"- {s}" for s in t["sample"])
        blocks.append(f"[{i}] чат «{t['chat']}», сообщений {t['n']}, "
                      f"реакций {t['reacts']}\n{sample}")
    resp = llm_client().chat.completions.create(
        model=cfg.get("model", "deepseek-chat"),
        messages=[{"role": "system", "content": CHAT_PROMPT_TEMPLATE},
                  {"role": "user", "content": "\n\n".join(blocks)}],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=1500,
    )
    out = {}
    data = json.loads(resp.choices[0].message.content)
    for item in data.get("threads", []):
        try:
            out[int(item["id"])] = {
                "title": str(item.get("title", "")).strip()[:80],
                "gist": str(item.get("gist", "")).strip()[:220],
            }
        except (KeyError, TypeError, ValueError):
            continue
    return out


def format_chat_block(threads, cfg):
    """Строки блока «Обсуждения в чатах» для дайджеста."""
    if not threads:
        return []
    names = name_chat_topics(threads, cfg)
    lines = []
    for i, t in enumerate(threads, 1):
        info = names.get(i) or {}
        title = info.get("title") or "Обсуждение"
        gist = f": {esc(info['gist'])}" if info.get("gist") else ""
        stats = (f"{fmt_n(t['n'], 'сообщение', 'сообщения', 'сообщений')} · "
                 f"{fmt_n(t['reacts'], 'реакция', 'реакции', 'реакций')}")
        lines.append(f"• <b>{esc(title)}</b>{gist} ({stats}) · "
                     f'<a href="{t["link"]}">{esc(t["chat"])}</a>')
    return lines


async def run_weather(client, cfg, args):
    """Режим --weather: алерт приходит только при резком ухудшении на завтра."""
    channels = cfg.get("weather_channels") or []
    if not channels:
        print("weather: weather_channels не заданы в config.yaml", file=sys.stderr)
        return
    posts, warnings = await fetch_posts(client, cfg, channels=channels)
    if warnings:
        print("weather: " + "; ".join(warnings), file=sys.stderr)
    if not posts:
        print("weather: свежих постов нет, молчим", file=sys.stderr)
        return

    user_msg = "\n\n".join(
        f"[{p['idx']}] ({p['channel']}, {p['time']})\n{p['text']}" for p in posts)
    resp = llm_client().chat.completions.create(
        model=cfg.get("model", "deepseek-chat"),
        messages=[{"role": "system", "content": WEATHER_PROMPT_TEMPLATE},
                  {"role": "user", "content": user_msg}],
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=600,
    )
    data = json.loads(resp.choices[0].message.content)
    if not data.get("alert"):
        print("weather: резких изменений не обещают, молчим", file=sys.stderr)
        return

    text = ("🌧 <b>Погода завтра резко портится</b>\n"
            + esc(str(data.get("message", "")).strip()))
    if args.dry_run:
        print(re.sub(r"</?[a-z][^>]*>", "", text))
        return
    target = "me" if args.to_me else cfg.get("weather_target", "me")
    for part in split_message(text):
        await client.send_message(target, part, parse_mode="html",
                                  link_preview=False)


def links_html(topic, by_idx, limit=2):
    out = []
    for pid in topic["post_ids"][:limit]:
        p = by_idx.get(pid)
        if p:
            out.append(f'<a href="{p["link"]}">{esc(p["channel"])}</a>')
    return " · ".join(out)


def build_digest(topics, posts, cfg, warnings, chat_lines=None):
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

    if chat_lines:
        lines += ["", "<b>Обсуждения в чатах:</b>", *chat_lines]

    total_chars = sum(p.get("full_len", len(p["text"])) for p in posts)
    saved_min = max(1, round(total_chars / READ_SPEED))
    wish = WISHES[datetime.now(MSK).timetuple().tm_yday % len(WISHES)]
    lines += ["", "\u2705 Вы сэкономили ~"
                  + fmt_n(saved_min, "минуту", "минуты", "минут")
                  + " на чтении постов",
              f"<i>{wish}</i>"]

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
    ap.add_argument("--to-me", action="store_true",
                    help="отправить в Избранное вместо target из конфига")
    ap.add_argument("--weather", action="store_true",
                    help="погодный дежурный: алерт только при резком ухудшении")
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

        if args.weather:
            await run_weather(client, cfg, args)
            await client.disconnect()
            return

        posts, warnings = await fetch_posts(client, cfg)

        chat_lines = []
        if cfg.get("chats"):
            try:
                threads, chat_warn = await fetch_chat_threads(client, cfg)
                warnings += chat_warn
                chat_lines = format_chat_block(threads, cfg)
            except Exception as e:
                # чаты не должны ронять новостной дайджест
                warnings.append(f"блок чатов пропущен: {e!r}")

        if not posts and not chat_lines:
            digest = "За сутки ни одного содержательного поста (после фильтра рекламы)."
            if warnings:
                digest += "\nПроблемы: " + esc("; ".join(warnings))
        else:
            topics = rank_topics(posts, cfg) if posts else []
            digest = build_digest(topics, posts, cfg, warnings, chat_lines)
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
        target = "me" if args.to_me else cfg.get("target", "me")
        try:
            for part in split_message(digest):
                await client.send_message(target, part, parse_mode="html",
                                          link_preview=False)
        except Exception as e:
            if target == "me":
                raise
            # канал недоступен (переименован, нет прав и т.п.):
            # дайджест не теряем, несём его в Избранное с пояснением
            note = (f"Не смог отправить в '{esc(str(target))}': "
                    f"{esc(repr(e))}. Дайджест ниже, проверьте target "
                    f"в config.yaml.")
            for part in split_message(note + "\n\n" + digest):
                await client.send_message("me", part, parse_mode="html",
                                          link_preview=False)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
