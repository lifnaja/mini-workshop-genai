import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def get_article_content(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(res.content, "html.parser")

        # ค้นหาเนื้อหาหลักของบทความ
        paragraphs = soup.select("div.ArticleBody-articleBody p")
        content = " ".join(p.get_text(strip=True) for p in paragraphs)

        return content if content else "No content found"
    except Exception as e:
        return f"Error fetching content: {e}"

def get_latest_world_news_with_content():
    url = "https://www.cnbc.com/world/"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    articles = soup.select("a.LatestNews-headline")
    news_list = []

    for article in articles:  # จำกัด 10 ข่าวล่าสุด เพื่อไม่โดน block
        headline = article.get_text(strip=True)
        link = article["href"]
        if not link.startswith("http"):
            link = "https://www.cnbc.com" + link

        print(f"Fetching: {headline}")
        # content = get_article_content(link)
        time.sleep(1)  # ถ้าไม่มี API, ควรใส่ delay ป้องกันโดน block

        news_list.append({
            "headline": headline,
            "url": link,
            # "content": content
        })

    print(len(news_list))
    return pd.DataFrame(news_list)

# ทดสอบ
if __name__ == "__main__":
    df = get_latest_world_news_with_content()
    # print(df[['headline', 'content']].head())
