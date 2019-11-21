#!/usr/bin/env python3
# screenshot.py

import asyncio
from pyppeteer import launch


async def main():
    browser = await launch()
    page = await browser.newPage()
    await page.goto('http://douyu.com')
    await page.screenshot({'path': 'example.png'})
    await browser.close()


if __name__ == '__main__':
    asyncio.run(main())
