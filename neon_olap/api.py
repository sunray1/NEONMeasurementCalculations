# neon_olap/api.py

from typing import List
import neonutilities as nu


def discover_urls(product: str, site: str, year: int, month: int) -> List[str]:
    """
    Fetch all file URLs for a given NEON product, site, year, and month.

    Returns a list of URLs (typically Google Cloud links to zip files).
    """

    start = f"{year}-{month:02d}"
    end = start

    urls, metadata = nu.zips_by_product(
        dpid=product,
        site=site,
        startdate=start,
        enddate=end,
        package="basic",
        cloud_mode=True,
        check_size=False,
        token=None,
    )

    print(f"[discover_urls] {product} {site} {start} → {len(urls)} files")

    return urls