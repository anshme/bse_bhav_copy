import re
from playwright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.samco.in/bhavcopy-nse-bse-mcx")
    page.locator("#start_date").fill("2025-08-30")
    page.locator("#end_date").fill("2025-09-06")
    page.locator("label").filter(has_text="NSE F&O").locator("span").click()
    page.locator("label").filter(has_text="BSE Cash").locator("span").click()
    page.locator("label").filter(has_text="MCX").locator("span").click()
    page.get_by_role("link", name="Download", exact=True).click()
    page.close()

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
