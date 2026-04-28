import os
import time
import requests
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from src.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

RIOT_API_KEY = os.getenv("RIOT_API_KEY")
REGION = os.getenv("REGION", "euw1")
ROUTING_REGION = os.getenv("ROUTING_REGION", "europe")

session = requests.Session()
session.headers.update({"X-Riot-Token": RIOT_API_KEY})

def _make_request(url: str) -> Optional[Dict[str, Any]]:
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            response = session.get(url, timeout=10)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logger.warning(f"Rate limited (429). Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
                retries += 1
                continue
            elif response.status_code == 404:
                logger.warning(f"404 Not Found for URL: {url}. Skipping.")
                return None
            else:
                logger.error(f"API Error {response.status_code} for URL: {url} | Response: {response.text}")
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            time.sleep(5)
            retries += 1

    logger.error(f"Max retries reached for {url}")
    return None

def get_challenger_ladder() -> List[Dict[str, Any]]:
    logger.info("Extracting Challenger ladder...")
    url = f"https://{REGION}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
    data = _make_request(url)
    
    if data and "entries" in data:
        entries = sorted(data["entries"], key=lambda x: x["leaguePoints"], reverse=True)
        return entries[:50]
    return []

def get_summoner_name_by_puuid(puuid: str) -> Optional[str]:
    url = f"https://{ROUTING_REGION}.api.riotgames.com/riot/account/v1/accounts/by-puuid/{puuid}"
    data = _make_request(url)
    if not data:
        return None
    game_name = data.get("gameName", "")
    tag_line = data.get("tagLine", "")
    return f"{game_name}#{tag_line}" if game_name else None

def get_top_champion_masteries(puuid: str, count: int = 3) -> List[Dict[str, Any]]:
    url = f"https://{REGION}.api.riotgames.com/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}/top?count={count}"
    data = _make_request(url)
    return data if data else []

def get_champion_mapping() -> dict:
    logger.info("Fetching latest Champion mapping from Data Dragon...")
    version_url = "https://ddragon.leagueoflegends.com/api/versions.json"
    latest_version = session.get(version_url).json()[0]

    data_url = f"https://ddragon.leagueoflegends.com/cdn/{latest_version}/data/en_US/champion.json"
    response = session.get(data_url).json()
    
    return {int(v['key']): v['name'] for k, v in response['data'].items()}