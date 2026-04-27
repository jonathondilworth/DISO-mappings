from __future__ import annotations

import argparse
import logging
import shutil
import yaml
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request

from pathlib import Path

from diso_mappings.paths import DISO_DIR
from diso_mappings.constants import DISO_REPO_LOCATION
from diso_mappings.io.terminal import highlight

log = logging.getLogger("download_diso")


def _get_tmp_fp(tmp_suffix: str, delete: bool = False) -> Path:
    tmp_open = tempfile.NamedTemporaryFile(suffix=tmp_suffix, delete=delete)
    tmp_path = Path(tmp_open.name)
    tmp_open.close()
    return tmp_path


def download_and_extract(repo: str, ref: str, dest_dir: Path) -> None:
    """
    Provided a GitHub repo name \w a ref (release), download and extract
    tarball members (the release) to the specified dest_dir path.
    """
    src_url = f"https://github.com/{repo}/archive/{ref}.tar.gz"
    log.info("Downloading from %s", src_url)
    dest_dir.mkdir(parents=True, exist_ok=True)

    tmp_download_fp = _get_tmp_fp(tmp_suffix=".tar.gz")

    try:
        with urllib.request.urlopen(src_url, timeout=60) as resp:
            with open(tmp_download_fp, "wb") as file:
                shutil.copyfileobj(resp, file)
        
        log.info("Downloaded DISO collection. Extracting.")

        with tarfile.open(tmp_download_fp, "r:gz") as tar:
            tar_members = tar.getmembers()
            if not tar_members:
                raise RuntimeError("You downloaded an empty archive!")
            
            top_level_member = tar_members[0].name.split("/")[0]

            for this_member in tar_members:
                if this_member.name == top_level_member:
                    continue

                if not this_member.name.startswith(top_level_member + "/"):
                    continue

                this_member.name = this_member.name[len(top_level_member)+1:]
                tar.extract(this_member, path=dest_dir)

    finally: # cleanup
        tmp_download_fp.unlink(missing_ok=True)        



def main() -> int:
    """
    TRY AND DOWNLOAD: DISO ontologies.
    """
    parser = argparse.ArgumentParser(
        description="downloads gh repo (for DISO)"
    )
    parser.add_argument(
        "--repo", default=None, help="Override repo (owner/name)"
    )
    parser.add_argument(
        "--ref", default=None, help="Override git ref"
    )
    parser.add_argument(
        "--verbose", action="store_true"
    )
    args = parser.parse_args()

    highlight("Starting download DISO script.")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    src_repository = args.repo if args.repo else DISO_REPO_LOCATION['repo']
    src_reference  = args.ref  if args.ref  else DISO_REPO_LOCATION['ref']

    try:
        download_and_extract(repo=src_repository, ref=src_reference, dest_dir=DISO_DIR)
    
    except urllib.error.URLError as e:
        log.error("Network error: %s", e)
        return 1

    except Exception as e:
        log.error("Download failed: %s: %s", type(e).__name__, e)
        return 1

    log.info("Downloaded DISO (%s @ %s) to %s", src_repository, src_reference, DISO_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
