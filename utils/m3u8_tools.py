# utils/m3u8_tools.py
import aiohttp
import m3u8
from typing import List, Dict

from utils.media_tools import run_ffmpeg


async def _fetch_m3u8(url: str) -> m3u8.M3U8:
    """
    Fetch m3u8 content asynchronously and parse with m3u8 lib.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            text = await resp.text()
    # uri param: base url for relative playlist/segment urls
    return m3u8.loads(text, uri=url)


async def get_m3u8_variants(url: str) -> List[Dict[str, str]]:
    """
    Returns list of variants:
    [ { "name": "360p", "url": "http://..." }, ... ]
    If no variants (simple playlist): returns one entry Auto.
    """
    playlist = await _fetch_m3u8(url)
    variants: List[Dict[str, str]] = []

    if playlist.playlists:  # master playlist with multiple qualities
        for pl in playlist.playlists:
            name = "Variant"
            if pl.stream_info:
                if pl.stream_info.resolution:
                    w, h = pl.stream_info.resolution
                    name = f"{h}p"
                elif pl.stream_info.bandwidth:
                    name = f"{pl.stream_info.bandwidth // 1000}kbps"
            variants.append(
                {
                    "name": name,
                    "url": pl.absolute_uri,
                }
            )
    else:
        variants.append(
            {
                "name": "Auto",
                "url": url,
            }
        )

    return variants


async def download_m3u8_stream(src_url: str, dest_path: str):
    """
    Download m3u8 stream using ffmpeg; container as mp4.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        src_url,
        "-c",
        "copy",
        dest_path,
    ]
    await run_ffmpeg(cmd)
