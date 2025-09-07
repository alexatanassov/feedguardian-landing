#!/usr/bin/env python3
from __future__ import annotations
import asyncio, json, os, re, sys, time, csv, pathlib, random, hashlib, urllib.parse
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

import typer
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from selectolax.parser import HTMLParser

app = typer.Typer(help="FeedGuardian parity/evidence crawler")

USER_AGENTS = [
    # rotate a few realistic desktop UAs
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
]

PRICE_PATTERNS = [
    r"\$\s?\d[\d,]*(?:\.\d{2})?",    # $1,234.56
    r"£\s?\d[\d,]*(?:\.\d{2})?",
    r"€\s?\d[\d,]*(?:\.\d{2})?",
]

AVAIL_KEYWORDS = {
    "in_stock": ["in stock", "available", "ships", "ready to ship", "add to cart"],
    "out_of_stock": ["out of stock", "sold out", "unavailable", "coming soon"],
}

RETURNS_HINTS = ["return", "refund", "exchange"]

def slugify(url: str) -> str:
    # keep deterministic folder names per PDP
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    parsed = urllib.parse.urlparse(url)
    base = pathlib.Path(parsed.path).name or "product"
    return f"{base}-{h}"

def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def find_json_ld(doc: HTMLParser) -> List[Dict[str, Any]]:
    out = []
    for n in doc.css('script[type="application/ld+json"]'):
        try:
            block = n.text(strip=True)
            if not block:
                continue
            data = json.loads(block)
            if isinstance(data, list):
                out.extend(data)
            else:
                out.append(data)
        except Exception:
            continue
    return out

def pick_first(dct: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for k in keys:
        v = dct.get(k)
        if v:
            return v
    return None

def regex_first(text: str, patterns: List[str]) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return m.group(0)
    return None

async def capture_single(url: str, outdir: pathlib.Path,
                         returns_url: Optional[str] = None,
                         timeout_ms: int = 25000,
                         headless: bool = True) -> Dict[str, Any]:
    ensure_dir(outdir)
    ua = random.choice(USER_AGENTS)

    evidence: Dict[str, Any] = {
        "url": url,
        "ts": int(time.time()),
        "title": None,
        "canonical": None,
        "visible_price": None,
        "visible_availability": None,
        "schema_product": None,   # raw schema snippet if found
        "schema_offer": None,
        "errors": [],
    }

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 900}
        )
        page = await context.new_page()
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            # attempt SPA render completion
            try:
                await page.wait_for_load_state("networkidle", timeout=timeout_ms//2)
            except PWTimeout:
                pass

            await page.wait_for_timeout(800)

            # — Screenshots (full, then specific crops if possible) —
            await page.screenshot(path=str(outdir / "00-full.png"), full_page=True)

            # Heuristic: price region
            price_region = page.get_by_text(re.compile(r"(\$|£|€)\s?\d[\d,]*(?:\.\d{2})?", re.I)).first
            if await price_region.count() > 0:
                await price_region.screenshot(path=str(outdir / "01-price.png"))
            else:
                css_price = page.locator(".price, .price-item, [data-price], [itemprop='price']").first
                if await css_price.count() > 0:
                    await css_price.screenshot(path=str(outdir / "01-price.png"))
                else:
                    atc = page.get_by_role("button", name=re.compile("add to cart|buy now", re.I)).first
                    if await atc.count() > 0:
                        await atc.screenshot(path=str(outdir / "01-price.png"))

            # Availability region → screenshot
            avail_region = page.get_by_text(
                re.compile(r"in stock|out of stock|unavailable|ships|sold out|backorder|preorder", re.I)
            ).first
            got_avail_shot = False
            if await avail_region.count() > 0:
                await avail_region.screenshot(path=str(outdir / "02-availability.png"))
                got_avail_shot = True

            # Common Shopify buttons/selectors
            atc_btn = page.locator(
                "button[name='add'], button[type='submit'], .product-form__submit, .btn--add-to-cart"
            ).first

            btn_enabled = None
            if await atc_btn.count() > 0:
                # try to infer enabled/disabled
                try:
                    disabled_attr = await atc_btn.get_attribute("disabled")
                    aria_disabled = await atc_btn.get_attribute("aria-disabled")
                    btn_enabled = (disabled_attr is None) and (aria_disabled not in ("true", "1"))
                except:
                    btn_enabled = None

                # if we didn't get an availability shot yet, grab the ATC area
                if not got_avail_shot:
                    await atc_btn.screenshot(path=str(outdir / "02-availability.png"))

            # Visible availability text (fallback using entire body)
            try:
                text_low = (await page.locator("body").inner_text()).lower()
                if any(k in text_low for k in ["out of stock","sold out","unavailable"]):
                    evidence["visible_availability"] = "OUT_OF_STOCK"
                elif any(k in text_low for k in ["in stock","available","ships","ready to ship","add to cart","add to bag","add to basket"]):
                    evidence["visible_availability"] = "IN_STOCK"
                elif btn_enabled is not None:
                    evidence["visible_availability"] = "IN_STOCK" if btn_enabled else "OUT_OF_STOCK"
                else:
                    evidence["visible_availability"] = None
            except:
                evidence["visible_availability"] = None

            # Variant dropdown
            variant = page.locator("select[name*='variant'], select, [role='listbox'], .product-form__variants").first
            if await variant.count() > 0:
                await variant.screenshot(path=str(outdir / "03-variant.png"))

            # Returns / refunds
            if returns_url:
                rpage = await context.new_page()
                try:
                    await rpage.goto(returns_url, wait_until="domcontentloaded", timeout=timeout_ms)
                    await rpage.screenshot(path=str(outdir / "04-returns.png"), full_page=True)
                finally:
                    await rpage.close()
            else:
                # try footer link
                footer = page.locator("footer").first
                if await footer.count() > 0:
                    text = (await footer.inner_text())[:500].lower()
                    if any(k in text for k in RETURNS_HINTS):
                        await footer.screenshot(path=str(outdir / "04-returns.png"))

            # Footer business info
            if await page.locator("footer").count() > 0:
                await page.locator("footer").first.screenshot(path=str(outdir / "05-footer.png"))

            # — Extract DOM text for heuristics —
            html = await page.content()
            doc = HTMLParser(html)

            # Title (Playwright + fallback to H1)
            try:
                title_text = await page.title()
                if title_text and len(title_text.strip()) > 0:
                    evidence["title"] = title_text.strip()
            except:
                evidence["title"] = None

            # Fallback: <h1> if title not found
            if not evidence["title"]:
                try:
                    h1 = await page.locator("h1").first.inner_text()
                    evidence["title"] = h1.strip() if h1 else None
                except Exception:
                    pass

            # Canonical (robust via Playwright and resolves relative urls)
            try:
                canon_href = await page.locator('link[rel="canonical"]').first.get_attribute("href")
                if canon_href:
                    from urllib.parse import urljoin
                    evidence["canonical"] = urljoin(page.url, canon_href)
                else:
                    evidence["canonical"] = None
            except:
                evidence["canonical"] = None

            # Visible price
            try:
                node_price = await page.locator(
                    ".price, .price-item, [data-price], [itemprop='price'], [data-product-price]"
                ).first.inner_text()
            except Exception:
                node_price = None

            if node_price:
                m = re.search(r"(?:\$|£|€)\s?\d[\d,]*(?:\.\d{2})?", node_price)
                evidence["visible_price"] = m.group(0) if m else None
            else:
                text_sample = await page.locator("body").inner_text()
                m = re.search(r"(?:\$|£|€)\s?\d[\d,]*(?:\.\d{2})?", text_sample)
                evidence["visible_price"] = m.group(0) if m else None

            # JSON-LD Product/Offer
            try:
                html = await page.content()
                doc = HTMLParser(html)
                blocks = find_json_ld(doc)
                for block in blocks:
                    # Normalize @type → string
                    t = block.get("@type")
                    t_str = ",".join(t) if isinstance(t, list) else (str(t) if t else "")
                    if "Product" in t_str:
                        evidence["schema_product"] = block
                        offer = pick_first(block, ["offers", "Offers"])
                        if isinstance(offer, list) and offer:
                            offer = offer[0]
                        evidence["schema_offer"] = offer
                        break
                # If no JSON-LD offer, try meta tags
                if not evidence["schema_offer"]:
                    try:
                        meta_price = await page.locator(
                            "meta[itemprop='price'], meta[property='product:price:amount']"
                        ).first.get_attribute("content")
                        meta_curr = await page.locator(
                            "meta[itemprop='priceCurrency'], meta[property='product:price:currency']"
                        ).first.get_attribute("content")
                    except Exception:
                        meta_price, meta_curr = None, None
                    if meta_price:
                        evidence["schema_offer"] = {"price": meta_price, "priceCurrency": meta_curr or None}
            except Exception as e:
                evidence["errors"].append(f"schema_error:{type(e).__name__}:{e}")

        except Exception as e:
            evidence["errors"].append(f"page_error:{type(e).__name__}:{e}")
        finally:
            await context.close()
            await browser.close()

    # Write evidence.json
    with open(outdir / "evidence.json", "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2, ensure_ascii=False)

    return evidence

@app.command()
def single(
    url: str = typer.Argument(..., help="Product page URL"),
    screenshots_dir: str = typer.Option("evidence", help="Base evidence dir"),
    returns_url: Optional[str] = typer.Option(None, help="Returns/refunds page URL"),
    headless: bool = typer.Option(True, help="Run browser headlessly"),
    timeout_ms: int = typer.Option(25000, help="Page timeout"),
):
    """
    Capture a single PDP → screenshots + evidence.json
    """
    target_dir = pathlib.Path(screenshots_dir) / slugify(url)
    typer.echo(f"[capture] {url} → {target_dir}")
    asyncio.run(capture_single(url, target_dir, returns_url, timeout_ms, headless))
    typer.echo("Done.")

@app.command()
def batch(
    csv_path: str = typer.Argument(..., help="CSV with columns: url,returns_url(optional)"),
    screenshots_dir: str = typer.Option("evidence", help="Base evidence dir"),
    headless: bool = typer.Option(True),
    timeout_ms: int = typer.Option(25000),
    concurrency: int = typer.Option(3, help="Parallel pages")
):
    """
    Batch capture via CSV (url,returns_url)
    """
    rows: List[Tuple[str, Optional[str]]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append((row["url"], row.get("returns_url") or None))

    async def runner():
        sem = asyncio.Semaphore(concurrency)
        async def run_one(u, ru):
            async with sem:
                outdir = pathlib.Path(screenshots_dir) / slugify(u)
                await capture_single(u, outdir, ru, timeout_ms, headless)

        await asyncio.gather(*[run_one(u, ru) for (u, ru) in rows])

    typer.echo(f"[batch] {len(rows)} rows")
    asyncio.run(runner())
    typer.echo("Batch done.")

if __name__ == "__main__":
    app()
