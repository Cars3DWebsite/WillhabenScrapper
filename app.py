"""Main application module for the Willhaben scraper API."""

import os
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
import re

import pytz
from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_, func, text
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PersistentPlaywright:
	"""Maintain a single Playwright browser/page for ultra-fast repeated scrapes."""

	def __init__(self):
		self._lock = threading.Lock()
		self._playwright = None
		self._browser = None
		self._context = None
		self._page = None
		self._cookies_accepted = False

	def _ensure_started(self):
		if self._playwright is None:
			self._playwright = sync_playwright().start()
			self._browser = self._playwright.chromium.launch(
				headless=True,
				args=[
					'--no-sandbox',
					'--disable-setuid-sandbox',
					'--disable-dev-shm-usage'
				]
			)
			self._context = self._browser.new_context(
				viewport={'width': 1920, 'height': 1080},
				user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
				locale='de-AT'
			)
			self._context.set_default_navigation_timeout(15000)
			self._context.set_default_timeout(5000)
			self._page = self._context.new_page()
			self._cookies_accepted = False

	@contextmanager
	def page(self):
		self._lock.acquire()
		try:
			self._ensure_started()
			yield self._page
		finally:
			self._lock.release()

	@property
	def cookies_accepted(self) -> bool:
		return self._cookies_accepted

	def mark_cookies_accepted(self):
		self._cookies_accepted = True

	def reset(self):
		with self._lock:
			if self._context is None:
				return
			try:
				if self._page:
					self._page.close()
			except Exception:
				pass
			self._page = self._context.new_page()
			self._cookies_accepted = False

	def shutdown(self):
		with self._lock:
			try:
				if self._page:
					self._page.close()
			except Exception:
				pass
			self._page = None

			if self._context:
				try:
					self._context.close()
				except Exception:
					pass
				self._context = None

			if self._browser:
				try:
					self._browser.close()
				except Exception:
					pass
				self._browser = None

			if self._playwright:
				try:
					self._playwright.stop()
				except Exception:
					pass
				self._playwright = None

			self._cookies_accepted = False


playwright_singleton = PersistentPlaywright()
atexit.register(playwright_singleton.shutdown)

app = Flask(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://localhost/carscraper')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_pre_ping': True,
    'pool_recycle': 300,
}

db = SQLAlchemy(app)

CET = pytz.timezone('Europe/Vienna')

FAST_SCRAPE_MAX_CARS = int(os.getenv('FAST_SCRAPE_MAX_CARS', '40'))
FAST_SCRAPE_INTERVAL_SECONDS = float(os.getenv('FAST_SCRAPE_INTERVAL_SECONDS', '0.01'))
PRIORITY_ENRICH_INTERVAL_SECONDS = float(os.getenv('PRIORITY_ENRICH_INTERVAL_SECONDS', '5'))
ENRICH_INTERVAL_SECONDS = float(os.getenv('ENRICH_INTERVAL_SECONDS', '30'))
CLEANUP_INTERVAL_SECONDS = float(os.getenv('CLEANUP_INTERVAL_SECONDS', str(24 * 60 * 60)))
POSTED_AT_HARD_OFFSET_HOURS = int(os.getenv('POSTED_AT_HARD_OFFSET_HOURS', '1'))
POSTED_AT_HARD_OFFSET = timedelta(hours=POSTED_AT_HARD_OFFSET_HOURS)

scheduler = BackgroundScheduler(timezone=CET)


def _shutdown_scheduler():
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        pass


atexit.register(_shutdown_scheduler)


# ============================================================================
# DATABASE MODELS
# ============================================================================


class Car(db.Model):
    __tablename__ = 'cars'

    id = db.Column(db.Integer, primary_key=True)
    listing_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    title = db.Column(db.String(500), nullable=False)
    price = db.Column(db.Numeric(10, 2))
    currency = db.Column(db.String(10), default='EUR')
    brand = db.Column(db.String(100), index=True)
    model = db.Column(db.String(100))
    year = db.Column(db.Integer)
    mileage = db.Column(db.Integer)
    fuel_type = db.Column(db.String(50))
    transmission = db.Column(db.String(50))
    location = db.Column(db.String(200))
    image_urls = db.Column(db.JSON)
    url = db.Column(db.Text, nullable=False)
    description = db.Column(db.Text)
    posted_at = db.Column(db.DateTime)
    first_seen_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'listing_id': self.listing_id,
            'title': self.title,
            'price': float(self.price) if self.price else None,
            'currency': self.currency,
            'brand': self.brand,
            'model': self.model,
            'year': self.year,
            'mileage': self.mileage,
            'fuel_type': self.fuel_type,
            'transmission': self.transmission,
            'location': self.location,
            'image_urls': self.image_urls,
            'url': self.url,
            'description': self.description,
            'posted_at': self.posted_at.isoformat() if self.posted_at else None,
            'first_seen_at': self.first_seen_at.isoformat() if self.first_seen_at else None,
            'last_seen_at': self.last_seen_at.isoformat() if self.last_seen_at else None,
            'is_active': self.is_active,
        }


class ScrapingLog(db.Model):
    __tablename__ = 'scraping_log'

    id = db.Column(db.Integer, primary_key=True)
    scrape_started_at = db.Column(db.DateTime, default=datetime.utcnow)
    scrape_completed_at = db.Column(db.DateTime)
    cars_found = db.Column(db.Integer, default=0)
    cars_added = db.Column(db.Integer, default=0)
    cars_updated = db.Column(db.Integer, default=0)
    status = db.Column(db.String(50))
    error_message = db.Column(db.Text)


# ============================================================================
# SCRAPER CLASS
# ============================================================================


class WillhabenScraper:
    """Scraper for willhaben.at car listings - optimized for high cadence."""

    BASE_URL = (
        "https://www.willhaben.at/iad/gebrauchtwagen/auto/gebrauchtwagenboerse"
        "?sfId=7d143874-1761-4044-a218-11dff1e99ccf"
        "&rows=30&isNavigation=true&DEALER=1&PRICE_TO=12000&page=1"
    )

    def __init__(self, max_cars: int = 100, full_image_scraping: bool = False,
                 playwright_session: PersistentPlaywright = None):
        self.max_cars = max_cars
        self.full_image_scraping = full_image_scraping
        self.playwright_session = playwright_session or playwright_singleton

    def scrape_listings(self) -> List[Dict[str, Any]]:
        """Scrape car listings from the Willhaben dealer search."""
        cars: List[Dict[str, Any]] = []

        try:
            with self.playwright_session.page() as page:
                logger.info(f"Navigating to {self.BASE_URL}")
                try:
                    page.goto(self.BASE_URL, wait_until="domcontentloaded", timeout=12000)
                except PlaywrightTimeout as timeout_err:
                    logger.warning(f"Navigation timed out: {timeout_err}")
                    self.playwright_session.reset()
                    return cars
                except Exception as nav_err:
                    logger.error(f"Navigation error: {nav_err}")
                    self.playwright_session.reset()
                    return cars

                if not self.playwright_session.cookies_accepted:
                    cookie_selectors = [
                        'button#didomi-notice-agree-button',
                        'button[data-testid="uc-accept-all-button"]',
                        'button:has-text("Akzeptieren")',
                        'button:has-text("Alle akzeptieren")'
                    ]
                    page.wait_for_timeout(200)
                    for selector in cookie_selectors:
                        try:
                            btn = page.query_selector(selector)
                            if btn and btn.is_visible():
                                btn.click()
                                page.wait_for_timeout(200)
                                self.playwright_session.mark_cookies_accepted()
                                logger.info(f"Accepted cookies via selector {selector}")
                                break
                        except Exception as cookie_err:
                            logger.debug(f"Cookie selector failed {selector}: {cookie_err}")
                    page.wait_for_timeout(100)

                try:
                    page.wait_for_selector('article', timeout=3000)
                except PlaywrightTimeout:
                    logger.warning("Listing grid did not render in time; resetting page")
                    self.playwright_session.reset()
                    return cars

                page.evaluate("window.scrollBy(0, window.innerHeight)")
                page.wait_for_timeout(150)

                all_car_links = page.query_selector_all('a[href*="/gebrauchtwagen/"]')
                logger.info(f"Found {len(all_car_links)} raw car links")

                car_listings: List[Dict[str, Any]] = []
                seen_ids = set()

                for link in all_car_links:
                    try:
                        href = link.get_attribute('href')
                        if not href:
                            continue

                        full_url = f"https://www.willhaben.at{href}" if not href.startswith('http') else href

                        id_match = re.search(r'[-/](\d{6,})(?:[/?]|$)', href) or re.search(r'(?:adId|insertId|entryId)=(\d+)', href)
                        if not id_match:
                            continue

                        listing_id = id_match.group(1)

                        if listing_id in seen_ids:
                            continue
                        if '/gebrauchtwagenboerse' in href or '/kategorie' in href:
                            continue

                        seen_ids.add(listing_id)
                        car_listings.append({
                            'link_element': link,
                            'url': full_url,
                            'listing_id': listing_id
                        })
                    except Exception as process_err:
                        logger.debug(f"Error processing link: {process_err}")
                        continue

                logger.info(f"Unique car listings collected: {len(car_listings)}")

                if not car_listings:
                    logger.warning("No car listings detected; resetting Playwright page")
                    self.playwright_session.reset()
                    return cars

                for idx, listing_data in enumerate(car_listings[:self.max_cars]):
                    try:
                        link_element = listing_data['link_element']
                        url = listing_data['url']
                        listing_id = listing_data['listing_id']
                        parent = None

                        try:
                            parent_handle = link_element.evaluate_handle(
                                'el => el.closest("article") || el.closest("[class*=\'Card\']") || el.closest("[class*=\'Item\']") || el.parentElement?.parentElement'
                            )
                            parent = parent_handle.as_element() if parent_handle else None
                            text_content = parent.inner_text() if parent else link_element.inner_text()
                            if parent_handle:
                                parent_handle.dispose()
                        except Exception:
                            text_content = link_element.inner_text()

                        link_text = link_element.inner_text().strip()
                        title = link_text if len(link_text) > 5 else (text_content.split('\n')[0] if text_content else '')
                        title = (title or f"Car Listing {listing_id}")[:500]

                        image_url = None
                        try:
                            img = link_element.query_selector('img')
                            if not img and parent:
                                img = parent.query_selector('img')

                            if not img:
                                try:
                                    img_handle = link_element.evaluate_handle('''el => {
                                        let container = el.closest('article') ||
                                                        el.closest('[class*="Card"]') ||
                                                        el.closest('[data-testid*="result"]') ||
                                                        el.parentElement?.parentElement;
                                        if (!container) return null;

                                        let img = container.querySelector('img');
                                        if (img) return img;

                                        let picture = container.querySelector('picture');
                                        if (picture) {
                                            img = picture.querySelector('img');
                                            if (img) return img;
                                        }

                                        return null;
                                    }''')
                                    img = img_handle.as_element() if img_handle else None
                                    if img_handle:
                                        img_handle.dispose()
                                except Exception as je:
                                    logger.debug(f"JS thumbnail lookup failed: {je}")

                            if img:
                                image_url = (
                                    img.get_attribute('src') or
                                    img.get_attribute('data-src') or
                                    img.get_attribute('data-lazy-src') or
                                    img.get_attribute('data-original') or
                                    img.get_attribute('data-lazy')
                                )

                                if not image_url:
                                    srcset = img.get_attribute('srcset')
                                    if srcset:
                                        parts = [segment.strip().split()[0] for segment in srcset.split(',') if segment.strip()]
                                        if parts:
                                            image_url = parts[0]

                                if image_url:
                                    if image_url.startswith('//'):
                                        image_url = f"https:{image_url}"
                                    elif image_url.startswith('/') and not image_url.startswith('//'):
                                        image_url = f"https://www.willhaben.at{image_url}"
                                    elif not image_url.startswith('http'):
                                        image_url = f"https://www.willhaben.at/{image_url.lstrip('/')}"

                                    lower_url = image_url.lower()
                                    if 'placeholder' in lower_url or 'icon' in lower_url or image_url.endswith('.svg'):
                                        image_url = None

                            if not image_url:
                                try:
                                    bg_image = link_element.evaluate("el => window.getComputedStyle(el).backgroundImage || ''")
                                    if bg_image and 'url(' in bg_image:
                                        bg_url = bg_image.split('url(')[-1].rstrip(')').strip("\"' ")
                                        if bg_url:
                                            if bg_url.startswith('//'):
                                                bg_url = f"https:{bg_url}"
                                            elif bg_url.startswith('/'):
                                                bg_url = f"https://www.willhaben.at{bg_url}"
                                            elif not bg_url.startswith('http'):
                                                bg_url = f"https://www.willhaben.at/{bg_url.lstrip('/')}"

                                            lower_bg = bg_url.lower()
                                            if 'placeholder' not in lower_bg and 'icon' not in lower_bg and not bg_url.endswith('.svg'):
                                                image_url = bg_url
                                except Exception as be:
                                    logger.debug(f"Background image lookup failed: {be}")

                        except Exception as e_img:
                            logger.debug(f"Thumbnail extraction error: {e_img}")

                        image_urls = [image_url] if image_url else []

                        price = self._extract_price(text_content)
                        year = self._extract_year(text_content)
                        mileage = self._extract_mileage(text_content)
                        location = self._extract_location(text_content)
                        posted_at = self._extract_posted_date(text_content)
                        brand, model = self._parse_brand_model(title)

                        car_data = {
                            'listing_id': listing_id,
                            'title': title,
                            'price': price,
                            'currency': 'EUR',
                            'brand': brand,
                            'model': model,
                            'year': year,
                            'mileage': mileage,
                            'fuel_type': None,
                            'transmission': None,
                            'location': location,
                            'image_urls': image_urls,
                            'url': url,
                            'description': text_content[:500] if text_content else title,
                            'posted_at': posted_at,
                        }

                        cars.append(car_data)
                        logger.info(f"✓ {idx + 1}/{min(len(car_listings), self.max_cars)} scraped")

                    except Exception as extract_err:
                        logger.error(f"✗ Error extracting car {idx + 1}: {extract_err}")
                        continue

                logger.info(f"Scraping completed: {len(cars)} cars extracted")

        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

        return cars

    def _extract_price(self, text: str) -> Optional[float]:
        price_patterns = [r'€\s*([\d.,]+)', r'([\d.,]+)\s*€']
        for pattern in price_patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    price_str = match.group(1).replace('.', '').replace(',', '.')
                    return float(price_str)
                except Exception:
                    continue
        return None

    def _extract_year(self, text: str) -> Optional[int]:
        match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text)
        if match:
            try:
                year = int(match.group(0))
                if 1990 <= year <= 2025:
                    return year
            except Exception:
                pass
        return None

    def _extract_mileage(self, text: str) -> Optional[int]:
        match = re.search(r'([\d.]+)\s*km', text, re.IGNORECASE)
        if match:
            try:
                mileage_str = match.group(1).replace('.', '')
                return int(mileage_str)
            except Exception:
                pass
        return None

    def _extract_location(self, text: str) -> Optional[str]:
        match = re.search(r'\b(\d{4}\s+[A-ZÄÖÜa-zäöüß\s-]+?)(?:\n|$)', text)
        if match:
            return match.group(1).strip()[:200]
        return None

    def _extract_posted_date(self, text: str) -> Optional[datetime]:
        cleaned = text.replace('\u00a0', ' ').replace(' Uhr', '')
        now_local = datetime.now(CET)

        try:
            explicit_pattern = re.search(
                r'(?:zuletzt\s+geändert|erstellt\s+am)\s*:?'  # label
                r'\s*(\d{1,2}\.\d{1,2}\.\d{4})'            # date
                r'(?:,\s*(\d{1,2}:\d{2}))?',                 # optional time
                cleaned,
                re.IGNORECASE
            )
            if explicit_pattern:
                date_part = explicit_pattern.group(1)
                time_part = explicit_pattern.group(2) or '00:00'
                dt_local = datetime.strptime(f"{date_part} {time_part}", "%d.%m.%Y %H:%M")
                return (CET.localize(dt_local) + POSTED_AT_HARD_OFFSET).replace(tzinfo=None)

            lowered = cleaned.lower()

            if 'vor' in lowered:
                rel_match = re.search(r'vor\s+(\d+)\s+minute[n]?', lowered)
                if rel_match:
                    return (now_local - timedelta(minutes=int(rel_match.group(1))) + POSTED_AT_HARD_OFFSET).replace(tzinfo=None)

                rel_match = re.search(r'vor\s+(\d+)\s+stunde[n]?', lowered)
                if rel_match:
                    return (now_local - timedelta(hours=int(rel_match.group(1))) + POSTED_AT_HARD_OFFSET).replace(tzinfo=None)

                rel_match = re.search(r'vor\s+(\d+)\s+tag[en]?', lowered)
                if rel_match:
                    return (now_local - timedelta(days=int(rel_match.group(1))) + POSTED_AT_HARD_OFFSET).replace(tzinfo=None)

            if 'heute' in lowered:
                return (now_local + POSTED_AT_HARD_OFFSET).replace(tzinfo=None)

            if 'gestern' in lowered:
                return (now_local - timedelta(days=1) + POSTED_AT_HARD_OFFSET).replace(tzinfo=None)

            fallback_pattern = re.search(
                r'(\d{1,2})\.(\d{1,2})\.(\d{4})(?:,\s*(\d{1,2}:\d{2}))?',
                cleaned
            )
            if fallback_pattern:
                day, month, year = map(int, fallback_pattern.group(1, 2, 3))
                time_part = fallback_pattern.group(4) or '00:00'
                dt_local = datetime.strptime(f"{day:02d}.{month:02d}.{year:04d} {time_part}", "%d.%m.%Y %H:%M")
                return (CET.localize(dt_local) + POSTED_AT_HARD_OFFSET).replace(tzinfo=None)

        except Exception as e:
            logger.debug(f"Error parsing posted date: {e}")

        return None

    def _parse_brand_model(self, title: str) -> tuple:
        common_brands = [
            'Abarth', 'Alfa Romeo', 'Aston Martin', 'Audi', 'Bentley', 'BMW', 'Bugatti',
            'Cadillac', 'Chevrolet', 'Chrysler', 'Citroën', 'Citroen', 'Cupra', 'Dacia',
            'Dodge', 'Ferrari', 'Fiat', 'Ford', 'Honda', 'Hummer', 'Hyundai', 'Infiniti',
            'Jaguar', 'Jeep', 'Kia', 'Lamborghini', 'Lancia', 'Land Rover', 'Lexus',
            'Maserati', 'Mazda', 'McLaren', 'Mercedes-Benz', 'Mercedes', 'MG', 'Mini',
            'Mitsubishi', 'Nissan', 'Opel', 'Peugeot', 'Porsche', 'Renault', 'Rolls-Royce',
            'Saab', 'Seat', 'Skoda', 'Smart', 'Subaru', 'Suzuki', 'Tesla', 'Toyota',
            'Volkswagen', 'VW', 'Volvo'
        ]

        title_upper = title.upper()

        for brand in common_brands:
            if brand.upper() in title_upper:
                pattern = re.compile(rf'\b{re.escape(brand)}\b', re.IGNORECASE)
                match = pattern.search(title)

                if match:
                    after_brand = title[match.end():].strip()
                    model_match = re.match(r'^[\s\-]*([A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)?)', after_brand)
                    if model_match:
                        model = model_match.group(1).strip()
                        model = re.sub(r'[^\w\s\-]', '', model).strip()
                        if model and len(model) > 1:
                            return brand, model

                return brand, None

        return None, None

    def scrape_car_details(self, page, car_url: str) -> Dict[str, Any]:
        details: Dict[str, Any] = {
            'images': [],
            'posted_at': None,
        }

        try:
            logger.info(f"Fetching detail page: {car_url}")
            page.goto(car_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1500)

            image_selectors = [
                'img[class*="gallery"]',
                '[class*="ImageGallery"] img',
                '[class*="Carousel"] img',
                '[data-testid*="image"] img',
                'picture img',
                '.image-gallery img'
            ]

            seen_urls = set()

            for selector in image_selectors:
                img_elements = page.query_selector_all(selector)
                for img in img_elements:
                    url = (
                        img.get_attribute('src')
                        or img.get_attribute('data-src')
                        or img.get_attribute('data-original')
                    )

                    if not url:
                        srcset = img.get_attribute('srcset')
                        if srcset:
                            urls = [s.strip().split()[0] for s in srcset.split(',') if s.strip()]
                            if urls:
                                url = urls[-1]

                    if url and url not in seen_urls:
                        if url.startswith('//'):
                            url = f"https:{url}"
                        elif url.startswith('/'):
                            url = f"https://www.willhaben.at{url}"

                        lower_url = url.lower()
                        if 'thumb' not in lower_url and 'icon' not in lower_url and not url.endswith('.svg'):
                            details['images'].append(url)
                            seen_urls.add(url)

            logger.info(f"Found {len(details['images'])} images for car")

            metadata_texts: List[str] = []
            metadata_selectors = [
                "text=/Zuletzt geändert/i",
                "text=/Erstellt am/i",
                '[data-testid*="metadata"]',
                '[class*="Meta"]',
                '[class*="Details"]'
            ]

            for selector in metadata_selectors:
                try:
                    nodes = page.query_selector_all(selector)
                    for node in nodes:
                        try:
                            metadata_texts.append(node.inner_text())
                        except Exception:
                            continue
                except Exception:
                    continue

            if metadata_texts:
                combined_text = "\n".join(metadata_texts)
                extracted_date = self._extract_posted_date(combined_text)
                if extracted_date:
                    details['posted_at'] = extracted_date

        except Exception as e:
            logger.error(f"Error scraping details from {car_url}: {str(e)}")

        details['images'] = details['images'][:10]
        return details


# ============================================================================
# BACKGROUND JOBS
# ============================================================================


def scrape_and_store_cars():
    """Fast scraping job - thumbnails only for speed."""
    with app.app_context():
        log_entry = ScrapingLog()
        db.session.add(log_entry)
        db.session.commit()

        try:
            logger.info("Starting FAST scraping job (thumbnails only)...")

            scraper = WillhabenScraper(max_cars=FAST_SCRAPE_MAX_CARS, full_image_scraping=False)
            scraped_cars = scraper.scrape_listings()

            log_entry.cars_found = len(scraped_cars)
            cars_added = 0
            cars_updated = 0
            newly_added_listing_ids: List[str] = []

            current_listing_ids = {car['listing_id'] for car in scraped_cars}

            for car_data in scraped_cars:
                existing_car = Car.query.filter_by(listing_id=car_data['listing_id']).first()

                if existing_car:
                    existing_car.last_seen_at = datetime.utcnow()
                    existing_car.is_active = True
                    existing_car.price = car_data.get('price')
                    existing_car.updated_at = datetime.utcnow()
                    if car_data.get('posted_at'):
                        existing_car.posted_at = car_data.get('posted_at')
                    if car_data.get('image_urls'):
                        existing_car.image_urls = car_data.get('image_urls')
                    cars_updated += 1
                else:
                    new_car = Car(**car_data)
                    db.session.add(new_car)
                    cars_added += 1
                    logger.info(f"🆕 NEW CAR: {car_data.get('title', 'Unknown')} - Posted: {car_data.get('posted_at', 'Unknown')}")
                    newly_added_listing_ids.append(car_data['listing_id'])

            if current_listing_ids and len(scraped_cars) > 10:
                inactive_count = Car.query.filter(
                    and_(
                        Car.listing_id.notin_(current_listing_ids),
                        Car.is_active == True
                    )
                ).update({'is_active': False}, synchronize_session=False)
                logger.info(f"Marked {inactive_count} cars as inactive")
            else:
                logger.warning("Skipping deactivation: Too few cars scraped or scrape failed")

            db.session.commit()

            log_entry.scrape_completed_at = datetime.utcnow()
            log_entry.cars_added = cars_added
            log_entry.cars_updated = cars_updated
            log_entry.status = 'success'
            db.session.commit()

            logger.info(f"Scraping completed: {cars_added} added, {cars_updated} updated, {len(scraped_cars)} total")

            if newly_added_listing_ids:
                try:
                    priority_enrich_latest(newly_added_listing_ids)
                except Exception as enrich_err:
                    logger.error(f"Priority enrichment failed: {enrich_err}")

        except Exception as e:
            logger.error(f"Scraping job failed: {str(e)}")
            log_entry.status = 'failed'
            log_entry.error_message = str(e)
            log_entry.scrape_completed_at = datetime.utcnow()
            db.session.commit()


def enrich_cars_with_images():
    """Background job to enrich cars with full image galleries."""
    with app.app_context():
        try:
            logger.info("Starting image enrichment job...")

            candidate_cars = Car.query.filter(
                Car.is_active == True
            ).order_by(Car.first_seen_at.desc()).limit(200).all()

            cars_needing_images = []
            for car in candidate_cars:
                urls = car.image_urls or []
                if not isinstance(urls, list):
                    continue
                if len(urls) <= 1:
                    cars_needing_images.append(car)
                if len(cars_needing_images) >= 20:
                    break

            if not cars_needing_images:
                logger.info("No cars need image enrichment")
                return

            logger.info(f"Found {len(cars_needing_images)} cars needing full images")

            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                )
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    locale='de-AT'
                )
                page = context.new_page()

                scraper = WillhabenScraper(max_cars=1, full_image_scraping=False)
                enriched_count = 0

                for car in cars_needing_images:
                    try:
                        logger.info(f"Enriching details for: {car.title[:50]}...")

                        details = scraper.scrape_car_details(page, car.url)
                        full_images = details.get('images', [])
                        posted_at = details.get('posted_at')

                        if full_images and len(full_images) > max(len(car.image_urls or []), 1):
                            car.image_urls = full_images
                            enriched_count += 1
                            logger.info(f"✓ Added {len(full_images)} images to {car.listing_id}")
                        else:
                            logger.debug(f"No additional images found for {car.listing_id}")

                        if posted_at and car.posted_at != posted_at:
                            car.posted_at = posted_at
                            logger.info(f"✓ Updated posted_at for {car.listing_id} -> {posted_at}")

                        car.updated_at = datetime.utcnow()

                    except Exception as e:
                        logger.error(f"Error enriching car {car.listing_id}: {str(e)}")
                        continue

                browser.close()

            db.session.commit()
            logger.info(f"Image enrichment completed: {enriched_count}/{len(cars_needing_images)} cars enriched")

        except Exception as e:
            logger.error(f"Image enrichment job failed: {str(e)}")
            db.session.rollback()


def priority_enrich_latest(listing_ids: Optional[List[str]] = None, max_items: int = 10):
    """Immediately enrich brand-new listings so latest cars appear complete."""
    if not listing_ids:
        return

    limited_ids = list(dict.fromkeys(listing_ids))[:max_items]
    logger.info(f"Priority enriching latest listings: {limited_ids}")

    with app.app_context():
        cars = Car.query.filter(Car.listing_id.in_(limited_ids)).all()
        if not cars:
            return

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
                )
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    locale='de-AT'
                )
                page = context.new_page()
                scraper = WillhabenScraper(max_cars=1, full_image_scraping=False)

                enriched = 0
                for car in cars:
                    try:
                        details = scraper.scrape_car_details(page, car.url)
                        images = details.get('images') or []
                        posted_at = details.get('posted_at')

                        if images and (not car.image_urls or len(car.image_urls) <= 1):
                            car.image_urls = images
                            logger.info(f"Priority: updated images for {car.listing_id}")

                        if posted_at and (car.posted_at is None or car.posted_at != posted_at):
                            car.posted_at = posted_at
                            logger.info(f"Priority: updated posted_at for {car.listing_id} -> {posted_at}")

                        car.updated_at = datetime.utcnow()
                        enriched += 1
                    except Exception as detail_err:
                        logger.error(f"Priority enrichment failed for {car.listing_id}: {detail_err}")
                        continue

                browser.close()

            db.session.commit()
            logger.info(f"Priority enrichment complete: {enriched}/{len(cars)} listings updated")

        except Exception as exc:
            logger.error(f"Priority enrichment error: {exc}")
            db.session.rollback()


def cleanup_inactive_cars():
    """Daily cleanup job to remove old inactive cars."""
    with app.app_context():
        try:
            logger.info("Starting daily cleanup job...")

            cutoff_date = datetime.utcnow() - timedelta(days=7)
            deleted_count = Car.query.filter(
                and_(
                    Car.is_active == False,
                    Car.last_seen_at < cutoff_date
                )
            ).delete()

            db.session.commit()
            logger.info(f"Cleanup completed: {deleted_count} cars removed")

        except Exception as e:
            logger.error(f"Cleanup job failed: {str(e)}")
            db.session.rollback()


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        db.session.execute(text('SELECT 1'))
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e)
        }), 500


@app.route('/api/cars', methods=['GET'])
def get_cars():
    """Get paginated list of cars - sorted by most recent first."""
    try:
        page_number = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        limit = min(limit, 100)

        query = Car.query.filter_by(is_active=True).order_by(
            Car.posted_at.desc().nulls_last(),
            Car.last_seen_at.desc(),
            Car.first_seen_at.desc()
        )
        pagination = query.paginate(page=page_number, per_page=limit, error_out=False)

        return jsonify({
            'cars': [car.to_dict() for car in pagination.items],
            'pagination': {
                'page': page_number,
                'limit': limit,
                'total': pagination.total,
                'pages': pagination.pages,
                'has_next': pagination.has_next,
                'has_prev': pagination.has_prev
            }
        }), 200
    except Exception as e:
        logger.error(f"Error in get_cars: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/<listing_id>', methods=['GET'])
def get_car(listing_id):
    """Get single car by listing ID."""
    try:
        car = Car.query.filter_by(listing_id=listing_id, is_active=True).first()
        if not car:
            return jsonify({'error': 'Car not found'}), 404
        return jsonify({'car': car.to_dict()}), 200
    except Exception as e:
        logger.error(f"Error in get_car: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/search', methods=['GET'])
def search_cars():
    """Search cars with filters."""
    try:
        brand = request.args.get('brand')
        model = request.args.get('model')
        min_price = request.args.get('min_price', type=float)
        max_price = request.args.get('max_price', type=float)
        min_year = request.args.get('min_year', type=int)
        max_year = request.args.get('max_year', type=int)
        page_number = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)

        query = Car.query.filter_by(is_active=True)

        if brand:
            query = query.filter(Car.brand.ilike(f'%{brand}%'))
        if model:
            query = query.filter(Car.model.ilike(f'%{model}%'))
        if min_price is not None:
            query = query.filter(Car.price >= min_price)
        if max_price is not None:
            query = query.filter(Car.price <= max_price)
        if min_year is not None:
            query = query.filter(Car.year >= min_year)
        if max_year is not None:
            query = query.filter(Car.year <= max_year)

        query = query.order_by(Car.first_seen_at.desc())
        limit = min(limit, 100)
        pagination = query.paginate(page=page_number, per_page=limit, error_out=False)

        return jsonify({
            'cars': [car.to_dict() for car in pagination.items],
            'filters': {
                'brand': brand,
                'model': model,
                'min_price': min_price,
                'max_price': max_price,
                'min_year': min_year,
                'max_year': max_year
            },
            'pagination': {
                'page': page_number,
                'limit': limit,
                'total': pagination.total,
                'pages': pagination.pages,
                'has_next': pagination.has_next,
                'has_prev': pagination.has_prev
            }
        }), 200
    except Exception as e:
        logger.error(f"Error in search_cars: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/latest', methods=['GET'])
def get_latest_car():
    """Get the single most recent car uploaded."""
    try:
        latest_car = Car.query.filter_by(is_active=True).order_by(
            Car.posted_at.desc().nulls_last(),
            Car.last_seen_at.desc(),
            Car.first_seen_at.desc()
        ).first()

        if not latest_car:
            return jsonify({'error': 'No cars found'}), 404

        return jsonify({
            'car': latest_car.to_dict(),
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error in get_latest_car: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/latest-five', methods=['GET'])
def get_latest_five_cars():
    """Return the five newest cars, ordered chronologically."""
    try:
        recent_cars = Car.query.filter_by(is_active=True).order_by(
            Car.posted_at.desc().nulls_last(),
            Car.last_seen_at.desc(),
            Car.first_seen_at.desc()
        ).limit(5).all()

        if not recent_cars:
            return jsonify({'cars': [], 'retrieved_at': datetime.utcnow().isoformat()}), 200

        ordered_cars = sorted(
            recent_cars,
            key=lambda car: car.posted_at or car.first_seen_at or datetime.min
        )

        return jsonify({
            'cars': [car.to_dict() for car in ordered_cars],
            'retrieved_at': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error in get_latest_five_cars: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/cars/recent', methods=['GET'])
def get_recent_cars():
    """Get most recently seen cars (within last 24 hours or most recent)."""
    try:
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        limit = request.args.get('limit', 20, type=int)
        limit = min(limit, 100)

        cars = Car.query.filter(
            and_(
                Car.is_active == True,
                Car.first_seen_at >= cutoff_time
            )
        ).order_by(
            Car.posted_at.desc().nulls_last(),
            Car.last_seen_at.desc(),
            Car.first_seen_at.desc()
        ).limit(limit).all()

        return jsonify({
            'cars': [car.to_dict() for car in cars],
            'count': len(cars),
            'cutoff_time': cutoff_time.isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error in get_recent_cars: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get scraping statistics."""
    try:
        total_cars = Car.query.filter_by(is_active=True).count()
        total_brands = db.session.query(func.count(func.distinct(Car.brand))).scalar()
        recent_scrape = ScrapingLog.query.order_by(ScrapingLog.scrape_started_at.desc()).first()

        stats = {
            'total_active_cars': total_cars,
            'total_brands': total_brands,
            'last_scrape': recent_scrape.scrape_started_at.isoformat() if recent_scrape else None,
            'last_scrape_status': recent_scrape.status if recent_scrape else None,
            'last_scrape_cars_found': recent_scrape.cars_found if recent_scrape else 0
        }

        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error in get_stats: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/api/trigger-scrape', methods=['POST'])
def trigger_scrape():
    """Manual trigger for scraping."""
    try:
        scrape_and_store_cars()
        return jsonify({'message': 'Scraping job triggered successfully'}), 200
    except Exception as e:
        logger.error(f"Error triggering scrape: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ============================================================================
# APPLICATION STARTUP
# ============================================================================

def configure_app():
    """Initialize the database tables and ensure scheduler is running."""
    with app.app_context():
        logger.info("Applying database migrations (create_all if needed)")
        db.create_all()

    if not scheduler.get_jobs():
        logger.info("Scheduling background jobs")
        scheduler.add_job(
            func=scrape_and_store_cars,
            trigger='interval',
            seconds=FAST_SCRAPE_INTERVAL_SECONDS,
            id='fast_scrape',
            max_instances=1,
            replace_existing=True
        )
        scheduler.add_job(
            func=priority_enrich_latest,
            trigger='interval',
            seconds=PRIORITY_ENRICH_INTERVAL_SECONDS,
            id='priority_enrich',
            max_instances=1,
            replace_existing=True
        )
        scheduler.add_job(
            func=enrich_cars_with_images,
            trigger='interval',
            seconds=ENRICH_INTERVAL_SECONDS,
            id='enrich',
            max_instances=1,
            replace_existing=True
        )
        scheduler.add_job(
            func=cleanup_inactive_cars,
            trigger='interval',
            seconds=CLEANUP_INTERVAL_SECONDS,
            id='cleanup',
            max_instances=1,
            replace_existing=True
        )

    if not scheduler.running:
        logger.info("Starting background scheduler")
        scheduler.start()


configure_app()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port)

