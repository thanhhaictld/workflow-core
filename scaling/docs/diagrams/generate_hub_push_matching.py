"""Regenerate the editable Excalidraw diagram and its PNG preview.

Run from the repository root with Python 3 and Pillow installed:
    python scaling/docs/diagrams/generate_hub_push_matching.py
"""

from __future__ import annotations

import json
import math
import random
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


HERE = Path(__file__).resolve().parent
WIDTH, HEIGHT = 1960, 1320
SCALE = 2
INK = "#172b4d"
MUTED = "#46556f"
BLUE = "#dceeff"
BLUE_EDGE = "#3478c0"
GREEN = "#dcf5e8"
GREEN_EDGE = "#26845c"
PURPLE = "#eee6ff"
PURPLE_EDGE = "#7453b5"
AMBER = "#fff0cf"
AMBER_EDGE = "#bd842a"
RED = "#ffe6e2"
RED_EDGE = "#bf544b"
BACKGROUND = "#fbfcff"

def first_font(*candidates: str) -> Path:
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return path
    raise FileNotFoundError("Install Segoe UI or DejaVu Sans to render the PNG")


FONT_PATH = first_font(
    "C:/Windows/Fonts/segoeui.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
)
BOLD_PATH = first_font(
    "C:/Windows/Fonts/segoeuib.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
)

elements: list[dict] = []
random.seed(90317)


def base(kind: str, x: float, y: float, width: float, height: float, **extra) -> dict:
    item = {
        "id": f"hubpush{len(elements):04d}",
        "type": kind,
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "angle": 0,
        "strokeColor": INK,
        "backgroundColor": "transparent",
        "fillStyle": "solid",
        "strokeWidth": 2,
        "strokeStyle": "solid",
        "roughness": 0,
        "opacity": 100,
        "groupIds": [],
        "frameId": None,
        "roundness": None,
        "seed": random.randint(1, 2**31 - 1),
        "version": 1,
        "versionNonce": random.randint(1, 2**31 - 1),
        "isDeleted": False,
        "boundElements": None,
        "updated": 1794614400000,
        "created": 1794614400000,
        "link": None,
        "locked": False,
    }
    item.update(extra)
    elements.append(item)
    return item


def rect(x: int, y: int, w: int, h: int, fill: str, edge: str, radius: int = 20,
         line_width: int = 3, dashed: bool = False) -> None:
    base("rectangle", x, y, w, h, backgroundColor=fill, strokeColor=edge,
         strokeWidth=line_width, strokeStyle="dashed" if dashed else "solid",
         roundness={"type": 3, "value": radius})


def txt(x: int, y: int, w: int, value: str, size: int = 25,
        color: str = INK, bold: bool = False, align: str = "left",
        line_height: float = 1.32) -> None:
    height = math.ceil(len(value.splitlines()) * size * line_height)
    base("text", x, y, w, height, strokeColor=color, strokeWidth=1,
         text=value, originalText=value, fontSize=size, fontFamily=2,
         textAlign=align, verticalAlign="top", containerId=None,
         lineHeight=line_height, autoResize=False,
         customData={"boldPreview": bold})


def arrow(x1: int, y1: int, x2: int, y2: int, color: str = INK,
          dashed: bool = False, width: int = 3) -> None:
    left, top = min(x1, x2), min(y1, y2)
    base("arrow", left, top, abs(x2 - x1), abs(y2 - y1), strokeColor=color,
         strokeWidth=width, strokeStyle="dashed" if dashed else "solid",
         points=[[x1 - left, y1 - top], [x2 - left, y2 - top]],
         lastCommittedPoint=None, startBinding=None, endBinding=None,
         startArrowhead=None, endArrowhead="arrow", elbowed=False)


def card(x: int, y: int, w: int, h: int, title: str, lines: list[str],
         fill: str, edge: str) -> None:
    rect(x, y, w, h, fill, edge)
    txt(x + 28, y + 25, w - 56, title, 30, INK, True)
    for index, line in enumerate(lines):
        txt(x + 32, y + 89 + index * 54, w - 64, "• " + line, 23, MUTED)


def step(x: int, title: str, lines: list[str], fill: str, edge: str) -> None:
    rect(x, 677, 390, 205, fill, edge, 16)
    txt(x + 22, 696, 346, title, 29, INK, True)
    for index, line in enumerate(lines):
        txt(x + 22, 748 + index * 37, 346, line, 21, MUTED)


def recovery(x: int, title: str, lines: list[str], fill: str, edge: str) -> None:
    rect(x, 1000, 540, 174, fill, edge, 16)
    txt(x + 24, 1019, 492, title, 27, INK, True)
    for index, line in enumerate(lines):
        txt(x + 24, 1075 + index * 34, 492, line, 20, MUTED)


txt(62, 34, 1830, "Hub-owned task matching and SSE push", 43, INK, True)
txt(64, 96, 1820,
    "Proposed protocol: ephemeral worker capacity meets durable PostgreSQL task leases",
    24, MUTED)

card(60, 178, 490, 340, "Remote worker lane", [
    "Local slots are the final guard",
    "One active matching partition",
    "Advertise free credits + skills",
    "Execute; REST heartbeat / commit",
], BLUE, BLUE_EDGE)
card(735, 178, 490, 340, "Hub matching owner", [
    "Fenced (hub, partition) lease",
    "Ephemeral sessions + credits",
    "Coalesced readiness + selection",
    "Reserve credit; claim; SSE push",
], PURPLE, PURPLE_EDGE)
card(1410, 178, 490, 340, "PostgreSQL authority", [
    "Tasks, owners, epochs, leases",
    "FIFO + resource permits",
    "Attempts + workflow history",
    "Committed notification hints",
], GREEN, GREEN_EDGE)

arrow(553, 269, 731, 269, BLUE_EDGE)
txt(563, 226, 170, "capacity", 20, BLUE_EDGE, True, "center")
arrow(731, 430, 553, 430, PURPLE_EDGE)
txt(557, 442, 170, "SSE task", 20, PURPLE_EDGE, True, "center")
arrow(1228, 269, 1406, 269, PURPLE_EDGE)
txt(1230, 226, 175, "SQL claim", 20, PURPLE_EDGE, True, "center")
arrow(1406, 430, 1228, 430, GREEN_EDGE)
txt(1230, 442, 175, "ready hint", 20, GREEN_EDGE, True, "center")

txt(63, 596, 1800, "Normal assignment path", 33, INK, True)
step(60, "1  Demand", [
    "Worker opens SSE stream.",
    "Live free slots become credits.",
], BLUE, BLUE_EDGE)
step(550, "2  Match", [
    "Committed hint meets credit.",
    "Owner reserves one credit.",
], PURPLE, PURPLE_EDGE)
step(1040, "3  Durable claim", [
    "SQL checks FIFO and permits.",
    "Writes worker + epoch + lease.",
], GREEN, GREEN_EDGE)
step(1530, "4  Push + execute", [
    "SSE carries committed lease.",
    "REST heartbeat and result.",
], AMBER, AMBER_EDGE)
arrow(452, 780, 546, 780, INK)
arrow(942, 780, 1036, 780, INK)
arrow(1432, 780, 1526, 780, INK)

txt(63, 931, 1800, "Recovery when delivery is uncertain", 33, INK, True)
recovery(60, "Hub fails after claim", [
    "SSE may or may not have arrived.",
    "Never immediately assign it again.",
], RED, RED_EDGE)
recovery(710, "Lease remains durable", [
    "Heartbeat / completion wins, else expiry.",
    "Old worker epoch is fenced on reclaim.",
], GREEN, GREEN_EDGE)
recovery(1360, "Takeover + reconcile", [
    "New owner gets fresh worker credits.",
    "Bounded scan finds ready / expired work.",
], BLUE, BLUE_EDGE)
arrow(603, 1087, 706, 1087, RED_EDGE, True)
arrow(1253, 1087, 1356, 1087, RED_EDGE, True)

rect(60, 1221, 1840, 66, "#edf2fb", "#b7c4d9", 14, 2)
txt(88, 1235, 1780,
    "Invariant: memory holds demand and transport; PostgreSQL authorizes execution; external effects need idempotency.",
    23, INK, True, "center")

scene = {
    "type": "excalidraw",
    "version": 2,
    "source": "https://excalidraw.com",
    "elements": elements,
    "appState": {"gridSize": None, "viewBackgroundColor": BACKGROUND},
    "files": {},
}
(HERE / "hub-push-matching.excalidraw").write_text(
    json.dumps(scene, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

image = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), BACKGROUND)
draw = ImageDraw.Draw(image)


def font(size: int, bold: bool = False):
    return ImageFont.truetype(str(BOLD_PATH if bold else FONT_PATH), size * SCALE)


for element in elements:
    kind = element["type"]
    x, y = round(element["x"] * SCALE), round(element["y"] * SCALE)
    w, h = round(element["width"] * SCALE), round(element["height"] * SCALE)
    color = element["strokeColor"]
    if kind == "rectangle":
        radius = 18 * SCALE if element["roundness"] else 0
        draw.rounded_rectangle((x, y, x + w, y + h), radius=radius,
                               fill=element["backgroundColor"], outline=color,
                               width=element["strokeWidth"] * SCALE)
    elif kind == "text":
        size = element["fontSize"]
        is_bold = element.get("customData", {}).get("boldPreview", False)
        face = font(size, is_bold)
        lines = element["text"].splitlines()
        step_y = size * element["lineHeight"] * SCALE
        for index, line in enumerate(lines):
            box = draw.textbbox((0, 0), line, font=face)
            actual_width = box[2] - box[0]
            tx = x if element["textAlign"] == "left" else x + (w - actual_width) // 2
            draw.text((tx, y + round(index * step_y)), line, font=face, fill=color,
                      stroke_width=0)
    elif kind == "arrow":
        first, last = element["points"]
        x1, y1 = x + first[0] * SCALE, y + first[1] * SCALE
        x2, y2 = x + last[0] * SCALE, y + last[1] * SCALE
        line_width = element["strokeWidth"] * SCALE
        if element["strokeStyle"] == "dashed":
            total = abs(x2 - x1)
            sign = 1 if x2 >= x1 else -1
            for start in range(0, total - 20 * SCALE, 16 * SCALE):
                a = x1 + sign * start
                b = x1 + sign * min(start + 9 * SCALE, total - 21 * SCALE)
                draw.line((a, y1, b, y2), fill=color, width=line_width)
        else:
            draw.line((x1, y1, x2, y2), fill=color, width=line_width)
        direction = 1 if x2 >= x1 else -1
        draw.polygon([(x2, y2), (x2 - direction * 20 * SCALE, y2 - 11 * SCALE),
                      (x2 - direction * 20 * SCALE, y2 + 11 * SCALE)], fill=color)

image.save(HERE / "hub-push-matching.png", optimize=True)
print(f"Wrote {len(elements)} Excalidraw elements and {image.width}x{image.height} PNG")
