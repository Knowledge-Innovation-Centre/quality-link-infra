"""Render the display image embedded in an EDC credential template.

The EDC schema requires every credential to carry at least one
`IndividualDisplay` with a rendered page image, so the template endpoint draws a
simple A4 page from the course data: a header band with the institution's name,
the course title, a few detail lines, and a footer with a small QualityLink logo
and a provenance note.
"""
import io
import logging
from functools import lru_cache
from typing import List, Optional

from PIL import Image, ImageDraw, ImageFont

from config import ASSETS_DIR

logger = logging.getLogger(__name__)

# A4 portrait at 150 dpi.
PAGE_WIDTH = 1240
PAGE_HEIGHT = 1754
MARGIN = 110

HEADER_MIN_HEIGHT = 260
HEADER_PADDING = 70
HEADER_COLOUR = (11, 34, 59)      # #0b223b, the frontend's --color-primary-dark
ACCENT_COLOUR = (31, 120, 189)    # #1f78bd, the frontend's --color-primary
TEXT_COLOUR = (17, 25, 40)
MUTED_COLOUR = (107, 114, 128)
FOOTER_TEXT_COLOUR = (203, 213, 225)
FOOTER_PADDING = 50

LOGO_PATH = ASSETS_DIR / "QualityLink_logo_horizontal-white.png"
LOGO_WIDTH = 220

FOOTER_NOTE = "Credential template generated using the QualityLink course data aggregator."

# fonts-dejavu-core in the backend image; covers Latin, Greek and Cyrillic titles.
_FONT_REGULAR = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
_FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"


@lru_cache(maxsize=None)
def _font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    try:
        return ImageFont.truetype(_FONT_BOLD if bold else _FONT_REGULAR, size)
    except OSError:
        # Outside the container (local dev without DejaVu): Pillow's bundled
        # scalable font, Latin only.
        return ImageFont.load_default(size=size)


@lru_cache(maxsize=1)
def _logo() -> Optional[Image.Image]:
    """The white logo, scaled down for the navy footer band."""
    try:
        logo = Image.open(LOGO_PATH).convert("RGBA")
    except OSError as e:
        logger.warning("QualityLink logo not found at %s: %s", LOGO_PATH, e)
        return None
    height = round(logo.height * LOGO_WIDTH / logo.width)
    return logo.resize((LOGO_WIDTH, height), Image.LANCZOS)


def _wrap(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> List[str]:
    """Greedy word wrap; words longer than a line are broken by character."""
    lines: List[str] = []
    for paragraph in text.splitlines() or [""]:
        line = ""
        for word in paragraph.split():
            candidate = f"{line} {word}".strip()
            if draw.textlength(candidate, font=font) <= max_width:
                line = candidate
                continue
            if line:
                lines.append(line)
            while draw.textlength(word, font=font) > max_width:
                cut = len(word)
                while cut > 1 and draw.textlength(word[:cut], font=font) > max_width:
                    cut -= 1
                lines.append(word[:cut])
                word = word[cut:]
            line = word
        lines.append(line)
    return lines


def _fit_lines(draw: ImageDraw.ImageDraw, text: str, font, max_width: int,
               max_lines: Optional[int]) -> List[str]:
    lines = _wrap(draw, text, font, max_width)
    if max_lines and len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1].rstrip() + " …"
    return lines


def _draw_block(
    draw: ImageDraw.ImageDraw,
    y: int,
    text: str,
    font,
    fill,
    max_lines: Optional[int] = None,
    spacing: float = 1.3,
) -> int:
    """Draw wrapped text starting at y; return the y below the block."""
    lines = _fit_lines(draw, text, font, PAGE_WIDTH - 2 * MARGIN, max_lines)
    line_height = round(font.size * spacing)
    for line in lines:
        draw.text((MARGIN, y), line, font=font, fill=fill)
        y += line_height
    return y


def _draw_header(draw: ImageDraw.ImageDraw, institution: Optional[str]) -> int:
    """Navy band with the institution's name; return its height."""
    font = _font(48, bold=True)
    lines = _fit_lines(draw, institution or "", font, PAGE_WIDTH - 2 * MARGIN, 3) if institution else []
    line_height = round(font.size * 1.25)
    height = max(HEADER_MIN_HEIGHT, len(lines) * line_height + 2 * HEADER_PADDING)
    draw.rectangle((0, 0, PAGE_WIDTH, height), fill=HEADER_COLOUR)
    y = (height - len(lines) * line_height) // 2
    for line in lines:
        draw.text((MARGIN, y), line, font=font, fill="white")
        y += line_height
    return height


def _draw_footer(page: Image.Image, draw: ImageDraw.ImageDraw) -> int:
    """Navy band with the small QualityLink logo and the provenance note; return its top y."""
    font = _font(22)
    logo = _logo()
    gap = 36
    text_x = MARGIN + (logo.width + gap if logo is not None else 0)
    max_width = PAGE_WIDTH - MARGIN - text_x
    lines = _wrap(draw, FOOTER_NOTE, font, max_width)
    # Narrow the box while the line count holds, so the lines come out even.
    while max_width > 100 and len(_wrap(draw, FOOTER_NOTE, font, max_width - 10)) == len(lines):
        max_width -= 10
        lines = _wrap(draw, FOOTER_NOTE, font, max_width)
    line_height = round(font.size * 1.35)
    text_height = len(lines) * line_height
    block_height = max(logo.height if logo is not None else 0, text_height)

    band_top = PAGE_HEIGHT - block_height - 2 * FOOTER_PADDING
    draw.rectangle((0, band_top, PAGE_WIDTH, PAGE_HEIGHT), fill=HEADER_COLOUR)
    top = band_top + FOOTER_PADDING
    if logo is not None:
        page.paste(logo, (MARGIN, top + (block_height - logo.height) // 2), logo)
    y = top + (block_height - text_height) // 2
    for line in lines:
        draw.text((text_x, y), line, font=font, fill=FOOTER_TEXT_COLOUR)
        y += line_height
    return band_top


def render_display_jpeg(
    title: str,
    institution: Optional[str],
    details: List[str],
    quality: int = 80,
) -> bytes:
    """Render the credential's display page as JPEG bytes."""
    page = Image.new("RGB", (PAGE_WIDTH, PAGE_HEIGHT), "white")
    draw = ImageDraw.Draw(page)

    header_height = _draw_header(draw, institution)
    footer_top = _draw_footer(page, draw)

    y = header_height + 130
    y = _draw_block(draw, y, title, _font(64, bold=True), TEXT_COLOUR, max_lines=6, spacing=1.2)

    y += 30
    draw.rectangle((MARGIN, y, MARGIN + 160, y + 8), fill=ACCENT_COLOUR)
    y += 60

    detail_font = _font(32)
    for line in details:
        if y > footer_top - 120:
            break
        y = _draw_block(draw, y, line, detail_font, MUTED_COLOUR, max_lines=2)
        y += 10

    buf = io.BytesIO()
    page.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()
