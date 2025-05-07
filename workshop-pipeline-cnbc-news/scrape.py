import feedparser
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def fetch_cnbc_rss():
    rss_url = "https://www.cnbc.com/id/100003114/device/rss/rss.html"
    feed = feedparser.parse(rss_url)

    news_items = []
    for entry in feed.entries[:10]:  # จำกัด 10 ข่าวเพื่อความเร็ว (เอาออกถ้าจะดึงทั้งหมด)
        news_items.append({
            'title': entry.title,
            'link': entry.link,
            'published': entry.published
        })
    return news_items

def scrape_article_content(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # CNBC ใช้ tag นี้สำหรับเนื้อข่าว
        paragraphs = soup.select('div.ArticleBody-articleBody p')
        content = "\n".join(p.get_text() for p in paragraphs if p.get_text(strip=True))
        return content
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return None

def get_latest_news_with_content():
    news_items = fetch_cnbc_rss()
    for news in news_items:
        print(f"📥 Fetching: {news['title']}")
        news['content'] = scrape_article_content(news['link'])
        time.sleep(1)  # ป้องกันไม่ให้โดน block
    return pd.DataFrame(news_items)

# การใช้งาน
if __name__ == "__main__":
    df = get_latest_news_with_content()
    pd.set_option('display.max_colwidth', 200)
    print(df[['title', 'published', 'content']].head())
