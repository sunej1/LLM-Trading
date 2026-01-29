import trafilatura


def get_article_excerpt(url: str, max_chars: int = 4000):
    """
    Fetch and extract readable article text using trafilatura.

    Returns:
        excerpt (str or None)
        status (str): "ok" or "failed"
    """
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None, "failed"

        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False
        )

        if not text:
            return None, "failed"

        return text[:max_chars], "ok"

    except Exception:
        return None, "failed"
