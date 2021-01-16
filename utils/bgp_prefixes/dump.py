import shutil
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from pyasn import mrtx
from pytz import utc


def download(url, path):
    print(f"Downloading {url} to {path}")
    with urllib.request.urlopen(url) as response:
        with open(path, "wb") as file:
            shutil.copyfileobj(response, file)


def rib_url(date):
    # Example URL:
    # http://archive.routeviews.org/bgpdata/2021.01/RIBS/rib.20210115.1400.bz2
    return "http://archive.routeviews.org/bgpdata/{}/RIBS/rib.{}.{}.bz2".format(
        date.strftime("%Y.%m"), date.strftime("%Y%m%d"), date.strftime("%H00")
    )


if __name__ == "__main__":
    # Download last hour's RIB.
    date = datetime.now(utc) - timedelta(hours=1)
    rib = Path(date.strftime("rib-%Y%m%d-%H00")).with_suffix(".bz2")
    if not rib.exists():
        download(rib_url(date), rib)

    # Parse the RIB.
    prefixes = mrtx.parse_mrt_file(str(rib), print_progress=True)
    with rib.with_suffix(".txt").open("w") as f:
        f.writelines((prefix + "\n" for prefix in prefixes.keys()))
