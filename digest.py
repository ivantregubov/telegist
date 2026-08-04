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
import sqlite3
import sys
import time
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
BATCH_POSTS = 50               # постов в одном запросе: размер ответа модели
                               # перестаёт зависеть от размера новостного дня
MEMORY_DB = BASE_DIR / "memory.db"
MEMORY_MIN_OVERLAP = 2         # сколько общих ключевых слов считать совпадением
MEMORY_MAX_HINTS = 12          # сколько прошлых тем максимум показать модели

# Служебные слова: в ключевые слова темы не попадают
STOPWORDS = {
    "или", "для", "как", "что", "это", "все", "уже", "при", "над", "под",
    "про", "год", "года", "году", "лет", "млн", "млрд", "тыс", "руб",
    "рублей", "процентов", "может", "будет", "будут", "было", "были",
    "россии", "российский", "российская", "новый", "новая", "новые",
    "после", "перед", "между", "около", "более", "менее", "самый",
}

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
3. Для тем со score 6 и выше заполни why: почему это влияет лично на него,
   одной сжатой фразой. Пояснение обязано быть КОРОЧЕ формулировки самой
   темы (title), это жёсткое требование.
4. Ничего не выдумывай, опирайся только на факты из постов.
5. Верни не больше 25 тем: одиночные малозначимые посты объединяй
   в сборные темы.

ТОН (обязательно):
- Пиши нейтрально и спокойно. Запрещены слова "рухнул", "обвал", "катастрофа",
  "шок", "паника", "тревожный сигнал" и подобная драматизация. Не "рубль
  рухнул", а "рубль ослаб на 3%".
- Отделяй свершившееся от прогнозов. Мнение аналитиков остаётся мнением:
  "аналитики допускают снижение", а не "рынок ждёт падение".
- В why указывай горизонт: это касается ближайших дней, месяцев или года.
- При равном score выбирай в главные тему, из которой следует что-то
  конструктивное, а не просто пугающее.

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


def llm_json(system, user, cfg, max_tokens, temperature=0.2, attempts=3):
    """Единая точка обращений к модели с ответом строго в JSON.

    Закрывает три известных сбоя:
    - режим размышлений V4 включён по умолчанию и может сжечь весь лимит
      токенов до начала ответа, поэтому выключаем его явно;
    - в JSON-режиме DeepSeek документирован редкий пустой ответ, поэтому
      пустой и не-JSON ответ повторяются с паузой;
    - обрезанный по лимиту ответ чинится до последней целой записи."""
    last_err = "нет ответа"
    for attempt in range(attempts):
        resp = llm_client().chat.completions.create(
            model=cfg.get("model", "deepseek-v4-flash"),
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            response_format={"type": "json_object"},
            temperature=temperature,
            max_tokens=max_tokens,
            extra_body={"thinking": {"type": "disabled"}},
        )
        choice = resp.choices[0]
        raw = (choice.message.content or "").strip()
        if choice.finish_reason == "length":
            print("warning: ответ модели обрезан по лимиту токенов",
                  file=sys.stderr)
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                cut = raw.rfind("},")
                if cut != -1:
                    try:
                        return json.loads(raw[: cut + 1] + "]}")
                    except json.JSONDecodeError:
                        pass
                last_err = "не-JSON ответ, начало: " + raw[:120]
        else:
            last_err = ("пустой ответ; возможные причины: нулевой баланс "
                        "DeepSeek, перегрузка API или устаревшее имя модели "
                        "в config.yaml")
        if attempt < attempts - 1:
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(
        f"модель не дала пригодный ответ за {attempts} попытки: {last_err}")


def rank_topics(posts, cfg, warnings=None):
    """Ранжирование постов пачками по BATCH_POSTS штук: большой новостной
    день больше не упирается в лимит длины одного ответа."""
    system = PROMPT_TEMPLATE.replace("{PROFILE}", cfg["profile"].strip())
    topics, failed = [], []

    for start in range(0, len(posts), BATCH_POSTS):
        batch = posts[start:start + BATCH_POSTS]
        user_msg = "\n\n".join(
            f"[{p['idx']}] ({p['channel']}, {p['time']})\n{p['text']}"
            for p in batch)
        try:
            data = llm_json(system, user_msg, cfg, max_tokens=4000)
        except RuntimeError as e:
            failed.append(str(e))
            continue
        for t in data.get("topics", []):
            try:
                t["score"] = int(float(t.get("score") or 0))
            except (TypeError, ValueError):
                t["score"] = 0
            t["post_ids"] = [i for i in t.get("post_ids", [])
                             if isinstance(i, int)]
            topics.append(t)

    if failed:
        if not topics:
            raise RuntimeError(failed[-1])
        if warnings is not None:
            warnings.append(f"не обработано пачек постов: {len(failed)} "
                            f"(последняя ошибка: {failed[-1]})")

    topics = merge_similar(topics)
    topics.sort(key=lambda t: -t["score"])
    return topics


def merge_similar(topics):
    """Склейка дублей между пачками: одна новость, попавшая в разные пачки,
    сливается в одну тему по пересечению ключевых слов заголовка."""
    merged = []
    for t in sorted(topics, key=lambda x: -x.get("score", 0)):
        kw = keywords(t.get("title", ""))
        target = None
        if kw:
            for m in merged:
                if len(kw & m["_kw"]) >= 2:
                    target = m
                    break
        if target:
            target["post_ids"] = sorted(set(target["post_ids"] + t["post_ids"]))
        else:
            t["_kw"] = kw
            merged.append(t)
    for m in merged:
        m.pop("_kw", None)
    return merged


REVISE_PROMPT = """Ты уточняешь оценки тем новостного дайджеста с учётом того,
что человек уже читал за последние недели.

ТЕМЫ ИЗ ПРОШЛЫХ ДАЙДЖЕСТОВ:
{SEEN}

Ниже сегодняшние темы с их оценками. Для каждой реши:
- если тема повторяет уже прочитанное и нового по сути нет (тот же факт,
  пересказ, продолжающийся фон) — сильно понизь score, до 0-2;
- если это реальное развитие сюжета (новое решение, новые числа, смена
  ситуации) — оставь оценку как есть, даже если тема звучит похоже;
- если тема с прошлым не связана — оставь оценку без изменений.

Ответь строго валидным json, только изменившиеся темы:
{"changes": [{"id": 3, "score": 1}]}"""


def revise_with_memory(topics, cfg):
    """Понижает оценки тем, которые уже звучали и не получили развития."""
    hints = recall_similar(topics, cfg)
    if not hints:
        return topics
    listing = "\n".join(f"[{i}] score {t['score']}: {t.get('title', '')}"
                        for i, t in enumerate(topics))
    system = REVISE_PROMPT.replace("{SEEN}", "\n".join(f"- {h}" for h in hints))
    try:
        changes = llm_json(system, listing, cfg, max_tokens=1000,
                           temperature=0.0).get("changes", [])
    except Exception as e:
        print(f"memory: пересмотр не удался ({e!r})", file=sys.stderr)
        return topics

    for ch in changes:
        try:
            i, new = int(ch["id"]), int(ch["score"])
        except (KeyError, TypeError, ValueError):
            continue
        if 0 <= i < len(topics) and new < topics[i]["score"]:
            topics[i]["score"] = max(0, new)  # память только понижает
    topics.sort(key=lambda t: -t["score"])
    return topics


def keywords(text):
    """Ключевые слова темы: длинные слова и числа, без служебных."""
    words = re.findall(r"[а-яёa-z0-9]{4,}", (text or "").lower())
    return {w for w in words if w not in STOPWORDS}


def memory_db(cfg):
    """Открывает базу памяти и чистит записи старше окна памяти."""
    con = sqlite3.connect(MEMORY_DB)
    con.execute("CREATE TABLE IF NOT EXISTS seen "
                "(day TEXT, title TEXT, kw TEXT)")
    con.execute("CREATE INDEX IF NOT EXISTS seen_day ON seen(day)")
    cutoff = (datetime.now(MSK)
              - timedelta(days=cfg.get("memory_days", 28))).strftime("%Y-%m-%d")
    con.execute("DELETE FROM seen WHERE day < ?", (cutoff,))
    con.commit()
    return con


def recall_similar(topics, cfg):
    """Ищет прошлые темы, пересекающиеся с сегодняшними по ключевым словам.
    Возвращает строки-подсказки для промпта."""
    try:
        con = memory_db(cfg)
    except sqlite3.Error as e:
        print(f"memory: база недоступна ({e!r}), работаем без памяти",
              file=sys.stderr)
        return []
    rows = con.execute("SELECT day, title, kw FROM seen").fetchall()
    con.close()

    today_kw = [keywords(t.get("title", "")) for t in topics]
    hints = {}
    for day, title, kw_raw in rows:
        past_kw = set((kw_raw or "").split())
        if not past_kw:
            continue
        for kw in today_kw:
            if len(kw & past_kw) >= MEMORY_MIN_OVERLAP:
                hints[title] = max(hints.get(title, ""), day)
                break
    return [f"{day}: {title}" for title, day in
            sorted(hints.items(), key=lambda x: -len(x[1]))[:MEMORY_MAX_HINTS]]


def remember(topics, cfg):
    """Сохраняет сегодняшние значимые темы в память."""
    keep = [t for t in topics if t.get("score", 0) >= cfg.get("worth_threshold", 4)]
    if not keep:
        return
    day = datetime.now(MSK).strftime("%Y-%m-%d")
    try:
        con = memory_db(cfg)
        con.executemany(
            "INSERT INTO seen VALUES (?, ?, ?)",
            [(day, t["title"], " ".join(sorted(keywords(t["title"]))))
             for t in keep])
        con.commit()
        con.close()
    except sqlite3.Error as e:
        print(f"memory: не удалось сохранить ({e!r})", file=sys.stderr)


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
    data = llm_json(CHAT_PROMPT_TEMPLATE, "\n\n".join(blocks), cfg,
                    max_tokens=1500)
    out = {}
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
    data = llm_json(WEATHER_PROMPT_TEMPLATE, user_msg, cfg,
                    max_tokens=600, temperature=0.0)
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


def fit_why(why, title):
    """Пояснение не длиннее формулировки темы. Если модель нарушила правило,
    оставляем первое предложение; если и оно длиннее темы, опускаем пояснение."""
    why = (why or "").strip()
    if not why:
        return ""
    limit = len(title)
    if len(why) <= limit:
        return why
    first = re.split(r"(?<=[.!?])\s+", why)[0]
    return first if len(first) <= limit else ""


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
        why = fit_why(main.get("why"), main["title"])
        if why:
            lines.append(esc(why))
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
            topics = rank_topics(posts, cfg, warnings) if posts else []
            if topics and cfg.get("memory_days", 28) > 0:
                topics = revise_with_memory(topics, cfg)
                # тестовые прогоны память не пишут: иначе завтрашний дайджест
                # сочтёт эти темы уже опубликованными
                if not (args.dry_run or args.to_me):
                    remember(topics, cfg)
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
