#!/usr/bin/env python3
import argparse
import json
import math
import os
import random
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont, ImageOps

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "Cron2JetFighterMontage/1.0 (local video pipeline)"


DEFAULT_QUERIES = [
    "F-16 Fighting Falcon aircraft filetype:bitmap",
    "F-15 Eagle aircraft filetype:bitmap",
    "F/A-18 Hornet aircraft filetype:bitmap",
    "F-22 Raptor aircraft filetype:bitmap",
    "F-35 Lightning II aircraft filetype:bitmap",
    "Eurofighter Typhoon aircraft filetype:bitmap",
]


def atomic_write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def load_json(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def cron2_context():
    keys = [
        "CRON2_RUN_ID",
        "CRON2_JOB_NAME",
        "CRON2_SCHEDULED_FOR",
        "CRON2_TRIGGER_TYPE",
        "CRON2_PREVIOUS_RUN_ID",
        "CRON2_PREVIOUS_STATUS",
    ]
    return {key: os.environ.get(key) for key in keys if os.environ.get(key)}


def classify_exception(exc):
    text = str(exc)
    lower = text.lower()
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 429:
            return "connectivity_rate_limited"
        if 500 <= exc.code <= 599:
            return "connectivity_server_error"
        if exc.code == 404:
            return "missing_source"
    if "urlopen error" in lower or "timed out" in lower or "connection" in lower:
        return "connectivity"
    if "no images" in lower or "not enough source" in lower:
        return "missing_sources"
    if "simulated crash" in lower:
        return "simulated_crash"
    return "pipeline_error"


def update_state(state_path, state, stage, status, **fields):
    now = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    state.setdefault("events", []).append({"at": now, "stage": stage, "status": status, **fields})
    state["current_stage"] = stage
    state["status"] = status
    state["updated_at"] = now
    state.setdefault("completed_stages", {})
    if status == "complete":
        state["completed_stages"][stage] = now
    if "error" in fields:
        state["last_error"] = fields["error"]
    if "failure_class" in fields:
        state["failure_class"] = fields["failure_class"]
    atomic_write_json(state_path, state)


def maybe_simulate_crash(stage, target):
    if target == stage:
        raise RuntimeError(f"simulated crash at {stage}")


def http_get_json(base, params, max_attempts=4):
    url = f"{base}?{urllib.parse.urlencode(params)}"
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code != 429 or attempt == max_attempts:
                raise
            time.sleep((1.7 ** attempt) + random.uniform(0.2, 1.0))
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempts:
                raise
            time.sleep((1.5 ** attempt) + random.uniform(0.2, 0.8))
    raise last_exc or RuntimeError(f"GET failed: {url}")


def http_download(url, dest, max_attempts=4):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=90) as resp, open(dest, "wb") as fh:
                fh.write(resp.read())
            return
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code != 429 or attempt == max_attempts:
                raise
            time.sleep((1.8 ** attempt) + random.uniform(0.2, 1.0))
        except Exception as exc:
            last_exc = exc
            if attempt == max_attempts:
                raise
            time.sleep((1.5 ** attempt) + random.uniform(0.2, 0.8))
    raise last_exc or RuntimeError(f"Download failed: {url}")


def slugify(value):
    value = value.lower()
    value = re.sub(r"^file:", "", value)
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")[:96] or "jet-fighter"


def strip_file_prefix(title):
    return re.sub(r"^File:", "", title or "").replace("_", " ")


def commons_search(query, limit):
    data = http_get_json(
        COMMONS_API,
        {
            "action": "query",
            "format": "json",
            "generator": "search",
            "gsrsearch": query,
            "gsrnamespace": 6,
            "gsrlimit": limit,
            "prop": "imageinfo",
            "iiprop": "url|mime|size|extmetadata",
            "iiurlwidth": 1800,
        },
    )
    pages = data.get("query", {}).get("pages", {})
    assets = []
    for page in pages.values():
        info = (page.get("imageinfo") or [{}])[0]
        mime = info.get("mime", "")
        if mime not in {"image/jpeg", "image/png"}:
            continue
        title = page.get("title")
        url = info.get("thumburl") or info.get("url")
        if not title or not url:
            continue
        ext = info.get("extmetadata") or {}
        assets.append(
            {
                "title": title,
                "display_title": clean_caption(strip_file_prefix(title)),
                "url": info.get("url"),
                "download_url": url,
                "mime": mime,
                "width": info.get("width"),
                "height": info.get("height"),
                "license": (ext.get("LicenseShortName") or {}).get("value"),
                "credit": strip_html((ext.get("Credit") or {}).get("value", "")),
                "source_query": query,
            }
        )
    return assets


def strip_html(value):
    value = re.sub(r"<[^>]+>", "", value or "")
    return re.sub(r"\s+", " ", value).strip()


def clean_caption(value):
    value = re.sub(r"\.(jpg|jpeg|png)$", "", value, flags=re.I)
    value = re.sub(r"\s+", " ", value).strip()
    value = value.replace("F- 16", "F-16").replace("F 16", "F-16")
    return value[:92]


def choose_assets(queries, count):
    seen = set()
    chosen = []
    for query in queries:
        for asset in commons_search(query, max(count * 2, 8)):
            key = asset["title"].lower()
            if key in seen:
                continue
            seen.add(key)
            chosen.append(asset)
            if len(chosen) >= count:
                return chosen
    return chosen


def guess_extension(asset):
    if asset["mime"] == "image/png":
        return ".png"
    return ".jpg"


def download_assets(assets, media_dir, target_count):
    media_dir.mkdir(parents=True, exist_ok=True)
    warnings = []
    downloaded = []
    for idx, asset in enumerate(assets, start=1):
        if len(downloaded) >= target_count:
            break
        filename = f"{idx:02d}-{slugify(asset['title'])}{guess_extension(asset)}"
        dest = media_dir / filename
        asset = dict(asset)
        asset["local_path"] = str(dest)
        try:
            if not dest.exists() or dest.stat().st_size == 0:
                http_download(asset["download_url"], dest)
            asset["bytes"] = dest.stat().st_size
            downloaded.append(asset)
        except Exception as exc:
            asset["download_error"] = str(exc)
            warnings.append(f"{asset['title']}: {exc}")
    return downloaded, warnings


def load_font(size, bold=False):
    candidates = [
        Path("C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf"),
    ]
    for path in candidates:
        if path.exists():
            return ImageFont.truetype(str(path), size)
    return ImageFont.load_default()


def cover_image(image, width, height, zoom=1.0, pan_x=0.0, pan_y=0.0):
    image = ImageOps.exif_transpose(image).convert("RGB")
    scale = max(width / image.width, height / image.height) * zoom
    resized = image.resize(
        (max(1, int(image.width * scale)), max(1, int(image.height * scale))),
        Image.Resampling.LANCZOS,
    )
    max_x = max(0, resized.width - width)
    max_y = max(0, resized.height - height)
    left = int(max_x * (0.5 + pan_x))
    top = int(max_y * (0.5 + pan_y))
    left = max(0, min(max_x, left))
    top = max(0, min(max_y, top))
    return resized.crop((left, top, left + width, top + height))


def draw_gradient_overlay(frame):
    overlay = Image.new("RGBA", frame.size, (0, 0, 0, 0))
    px = overlay.load()
    width, height = frame.size
    for y in range(height):
        bottom_alpha = int(max(0, (y - height * 0.48) / (height * 0.52)) * 185)
        top_alpha = int(max(0, (height * 0.18 - y) / (height * 0.18)) * 85)
        alpha = max(bottom_alpha, top_alpha)
        for x in range(width):
            px[x, y] = (0, 0, 0, alpha)
    return Image.alpha_composite(frame.convert("RGBA"), overlay).convert("RGB")


def draw_text_box(frame, title, subtitle, progress):
    frame = draw_gradient_overlay(frame)
    draw = ImageDraw.Draw(frame)
    width, height = frame.size
    title_font = load_font(max(28, int(width * 0.045)), bold=True)
    body_font = load_font(max(18, int(width * 0.026)))
    mono_font = load_font(max(15, int(width * 0.02)))

    margin = int(width * 0.07)
    box_w = int(width * 0.86)
    y = int(height * 0.73)
    wrapped_title = wrap_text(title, title_font, box_w - 56)
    wrapped_sub = wrap_text(subtitle, body_font, box_w - 56)
    line_h_title = title_font.size + 8
    line_h_body = body_font.size + 8
    box_h = 42 + len(wrapped_title) * line_h_title + len(wrapped_sub) * line_h_body + 38

    draw.rounded_rectangle(
        [margin, y, margin + box_w, y + box_h],
        radius=18,
        fill=(8, 12, 18, 215),
        outline=(245, 245, 245, 60),
        width=1,
    )
    draw.text((margin + 28, y + 22), "JET FIGHTER PHOTO DOSSIER", fill=(164, 210, 255), font=mono_font)
    cursor = y + 58
    for line in wrapped_title:
        draw.text((margin + 28, cursor), line, fill=(250, 252, 255), font=title_font)
        cursor += line_h_title
    cursor += 4
    for line in wrapped_sub:
        draw.text((margin + 28, cursor), line, fill=(214, 222, 232), font=body_font)
        cursor += line_h_body

    bar_y = y + box_h + 26
    draw.rounded_rectangle([margin, bar_y, margin + box_w, bar_y + 8], radius=4, fill=(255, 255, 255, 60))
    draw.rounded_rectangle(
        [margin, bar_y, margin + int(box_w * progress), bar_y + 8],
        radius=4,
        fill=(85, 180, 255, 230),
    )
    return frame


def wrap_text(text, font, max_width):
    words = (text or "").split()
    lines = []
    current = ""
    probe = Image.new("RGB", (10, 10))
    draw = ImageDraw.Draw(probe)
    for word in words:
        candidate = f"{current} {word}".strip()
        if draw.textbbox((0, 0), candidate, font=font)[2] <= max_width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines[:3]


def render_video(assets, out_mp4, width, height, fps, seconds_per_photo):
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    writer = cv2.VideoWriter(str(out_mp4), fourcc, fps, (width, height))
    if not writer.isOpened():
        raise RuntimeError("OpenCV could not open MP4 writer")

    frames_per_photo = max(1, int(fps * seconds_per_photo))
    title = Image.new("RGB", (width, height), (6, 10, 16))
    title = draw_title_card(title, len(assets))
    for _ in range(max(1, int(fps * 1.5))):
        writer.write(cv2.cvtColor(np.array(title), cv2.COLOR_RGB2BGR))

    for idx, asset in enumerate(assets, start=1):
        with Image.open(asset["local_path"]) as raw:
            pan_seed = random.Random(asset["title"])
            pan_x = pan_seed.uniform(-0.14, 0.14)
            pan_y = pan_seed.uniform(-0.10, 0.10)
            for frame_idx in range(frames_per_photo):
                t = frame_idx / max(1, frames_per_photo - 1)
                ease = 0.5 - 0.5 * math.cos(t * math.pi)
                zoom = 1.02 + 0.08 * ease
                frame = cover_image(raw, width, height, zoom=zoom, pan_x=pan_x * ease, pan_y=pan_y * ease)
                subtitle = f"Image {idx} of {len(assets)}"
                if asset.get("license"):
                    subtitle += f" | {asset['license']}"
                frame = draw_text_box(frame, asset["display_title"], subtitle, idx / len(assets))
                writer.write(cv2.cvtColor(np.array(frame), cv2.COLOR_RGB2BGR))

    end = Image.new("RGB", (width, height), (6, 10, 16))
    end = draw_end_card(end)
    for _ in range(max(1, int(fps * 1.0))):
        writer.write(cv2.cvtColor(np.array(end), cv2.COLOR_RGB2BGR))

    writer.release()


def draw_title_card(frame, count):
    draw = ImageDraw.Draw(frame)
    width, height = frame.size
    title_font = load_font(max(36, int(width * 0.065)), bold=True)
    body_font = load_font(max(22, int(width * 0.032)))
    mono_font = load_font(max(16, int(width * 0.022)))
    draw.text((int(width * 0.07), int(height * 0.18)), "JET FIGHTER", fill=(246, 249, 255), font=title_font)
    draw.text((int(width * 0.07), int(height * 0.18) + title_font.size + 12), "PHOTO MONTAGE", fill=(95, 190, 255), font=title_font)
    draw.text(
        (int(width * 0.07), int(height * 0.38)),
        f"{count} archival and operational aircraft images from Wikimedia Commons",
        fill=(218, 226, 236),
        font=body_font,
    )
    draw.text((int(width * 0.07), int(height * 0.82)), "LOCAL PIPELINE RENDER", fill=(154, 174, 190), font=mono_font)
    return frame


def draw_end_card(frame):
    draw = ImageDraw.Draw(frame)
    width, height = frame.size
    title_font = load_font(max(34, int(width * 0.055)), bold=True)
    body_font = load_font(max(20, int(width * 0.03)))
    draw.text((int(width * 0.07), int(height * 0.32)), "END OF SORTIE", fill=(246, 249, 255), font=title_font)
    draw.text(
        (int(width * 0.07), int(height * 0.42)),
        "Manifest includes source URLs, licenses, and downloaded asset paths.",
        fill=(218, 226, 236),
        font=body_font,
    )
    return frame


def main():
    parser = argparse.ArgumentParser(description="Create a jet fighter photo montage MP4 from Wikimedia Commons images.")
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--state-dir")
    parser.add_argument("--photos", type=int, default=8)
    parser.add_argument("--seconds-per-photo", type=float, default=3.0)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--width", type=int, default=1080)
    parser.add_argument("--height", type=int, default=1920)
    parser.add_argument("--query", action="append", default=[])
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--simulate-offline", action="store_true")
    parser.add_argument("--simulate-missing-sources", action="store_true")
    parser.add_argument(
        "--simulate-crash-at",
        choices=["after-search", "after-download", "before-render", "after-render"],
    )
    args = parser.parse_args()

    random.seed(args.seed)
    outdir = Path(args.outdir)
    state_dir = Path(args.state_dir) if args.state_dir else outdir / ".state"
    state_path = state_dir / "state.json"
    sources_path = state_dir / "sources.json"
    media_dir = state_dir / "media-cache"
    render_cache = state_dir / "jet-fighter-montage.mp4"
    run_media_dir = outdir / "media"
    outdir.mkdir(parents=True, exist_ok=True)
    state = load_json(
        state_path,
        {
            "kind": "jet-fighter-photo-montage-state",
            "status": "new",
            "completed_stages": {},
            "events": [],
        },
    )
    state["cron2"] = cron2_context()
    state["requested"] = {
        "photos": args.photos,
        "width": args.width,
        "height": args.height,
        "fps": args.fps,
        "seconds_per_photo": args.seconds_per_photo,
    }
    state["recovery_mode"] = state.get("cron2", {}).get("CRON2_PREVIOUS_STATUS") in {"failed", "interrupted", "timed_out"}
    update_state(state_path, state, "startup", "running")

    queries = ["no-such-source-cron2-hardening-test"] if args.simulate_missing_sources else (args.query or DEFAULT_QUERIES)
    started = time.time()
    warnings = []

    try:
        update_state(state_path, state, "source_search", "running", queries=queries)
        cached_sources = load_json(sources_path, {}).get("assets", [])
        if args.simulate_offline:
            if cached_sources:
                assets = cached_sources
                warnings.append("simulated offline mode: reused cached source list")
            else:
                raise urllib.error.URLError("simulated offline mode with no cached sources")
        elif cached_sources and state.get("completed_stages", {}).get("source_search"):
            assets = cached_sources
        else:
            assets = choose_assets(queries, max(args.photos * 4, args.photos))
            if not assets:
                raise RuntimeError("not enough source candidates were found")
            atomic_write_json(sources_path, {"queries": queries, "assets": assets})
        update_state(state_path, state, "source_search", "complete", candidates=len(assets))
        maybe_simulate_crash("after-search", args.simulate_crash_at)

        update_state(state_path, state, "download", "running")
        downloaded, download_warnings = download_assets(assets, media_dir, args.photos)
        warnings.extend(download_warnings)
        if not downloaded:
            raise RuntimeError("No images were downloaded; cannot render montage.")
        run_media_dir.mkdir(parents=True, exist_ok=True)
        for asset in downloaded:
            src = Path(asset["local_path"])
            dest = run_media_dir / src.name
            if src.resolve() != dest.resolve():
                shutil.copy2(src, dest)
            asset["run_local_path"] = str(dest)
        state["downloaded_assets"] = downloaded
        update_state(state_path, state, "download", "complete", downloaded=len(downloaded), warnings=len(warnings))
        maybe_simulate_crash("after-download", args.simulate_crash_at)

        out_mp4 = outdir / "jet-fighter-montage.mp4"
        maybe_simulate_crash("before-render", args.simulate_crash_at)
        if render_cache.exists() and state.get("completed_stages", {}).get("render"):
            shutil.copy2(render_cache, out_mp4)
            update_state(state_path, state, "render", "complete", reused_cached_video=True)
        else:
            update_state(state_path, state, "render", "running")
            render_video(downloaded, out_mp4, args.width, args.height, args.fps, args.seconds_per_photo)
            shutil.copy2(out_mp4, render_cache)
            update_state(state_path, state, "render", "complete", video_bytes=out_mp4.stat().st_size)
        maybe_simulate_crash("after-render", args.simulate_crash_at)

        manifest = {
            "kind": "jet-fighter-photo-montage",
            "queries": queries,
            "requested_photos": args.photos,
            "downloaded_photos": len(downloaded),
            "warnings": warnings,
            "video": str(out_mp4),
            "video_bytes": out_mp4.stat().st_size,
            "width": args.width,
            "height": args.height,
            "fps": args.fps,
            "seconds_per_photo": args.seconds_per_photo,
            "duration_estimate_seconds": round(1.5 + len(downloaded) * args.seconds_per_photo + 1.0, 2),
            "elapsed_seconds": round(time.time() - started, 3),
            "assets": downloaded,
            "recovery": {
                "state": str(state_path),
                "state_dir": str(state_dir),
                "recovery_mode": state.get("recovery_mode", False),
                "cron2": state.get("cron2", {}),
                "completed_stages": state.get("completed_stages", {}),
            },
        }
        manifest_path = outdir / "montage-manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        update_state(state_path, state, "manifest", "complete", manifest=str(manifest_path))
        update_state(state_path, state, "pipeline", "complete", video=str(out_mp4))
        print(json.dumps({"video": str(out_mp4), "manifest": str(manifest_path), "downloaded": len(downloaded)}, indent=2))
    except Exception as exc:
        update_state(
            state_path,
            state,
            "pipeline",
            "failed",
            error=str(exc),
            failure_class=classify_exception(exc),
        )
        raise


if __name__ == "__main__":
    main()
