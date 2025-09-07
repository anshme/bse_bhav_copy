import asyncio
import zipfile
from playwright.async_api import async_playwright
from constants import CSV_FOLDER, COMPRESSED_DATA_DIR, logger
import os
import time


class Crawler:
    def __init__(self):
        self.URL = "https://www.samco.in/bhavcopy-nse-bse-mcx"

    async def download_and_extract(self, from_date, to_date):
        # Ensure download directory exists
        os.makedirs(COMPRESSED_DATA_DIR, exist_ok=True)

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)  # Change to True if you want headless
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()

            # Go to website
            await page.goto(self.URL)
            logger.info(f"Navigated to {self.URL}")
            await page.wait_for_selector("#start_date")
            time.sleep(1)  # Extra wait to ensure page is fully loaded
            # Fill the date fields
            logger.info(f"Setting the start date {from_date}")
            await page.locator("#start_date").fill(from_date)

            await page.wait_for_selector("#end_date")
            logger.info(f"Setting the end date {to_date}")
            await page.locator("#end_date").fill(to_date)
            time.sleep(1)

            # Select segments
            await page.locator("label").filter(has_text="NSE F&O").locator("span").click()
            time.sleep(0.5)
            await page.locator("label").filter(has_text="BSE Cash").locator("span").click()
            time.sleep(0.5)
            await page.locator("label").filter(has_text="MCX").locator("span").click()
            time.sleep(0.5)
            logger.info("Unselected all except NSE bhav copy")
            # Click on Download and wait for the file
            try:
                async with page.expect_download(timeout=120000) as download_info:  # 2 min timeout
                    await page.get_by_role("link", name="Download", exact=True).click()

                download = await download_info.value

                filename = f"bhavcopy_{from_date}_to_{to_date}.zip"
                save_path = os.path.join(COMPRESSED_DATA_DIR, filename)
                zip_path = os.path.join(COMPRESSED_DATA_DIR, filename)

                await download.save_as(save_path)
                print(f"Download successful: {save_path}")

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(CSV_FOLDER)
                print(f"Extracted to: {CSV_FOLDER}")

                os.remove(zip_path)
                print(f"Deleted ZIP file: {zip_path}")

            except Exception as e:
                print(f"Download failed: {e}")

            await browser.close()

    def crawl(self, from_date, to_date):
        asyncio.run(self.download_and_extract(from_date, to_date))
