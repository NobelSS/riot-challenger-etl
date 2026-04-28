import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.logger import get_logger
from src.extract import (
    get_challenger_ladder, 
    get_summoner_name_by_puuid, 
    get_top_champion_masteries, 
    get_champion_mapping
)
from src.transform import transform_data, add_champion_names
from src.load import load_to_parquet

logger = get_logger(__name__)

def get_player_data(player: dict, rank_idx: int) -> list:
    puuid = player.get("puuid")
    if not puuid:
        logger.warning(f"No PUUID for rank {rank_idx}. Skipping.")
        return []

    summoner_name = get_summoner_name_by_puuid(puuid) or f"Unknown_PUUID_{puuid[:5]}"
    masteries = get_top_champion_masteries(puuid, count=3)
    
    player_records = []
    for mastery in masteries:
        player_records.append({
            "summoner_name": summoner_name,
            "ladder_rank": rank_idx,
            "champion_id": mastery.get("championId"),
            "mastery_level": mastery.get("championLevel"),
            "total_mastery_points": mastery.get("championPoints"),
            "last_played_ms": mastery.get("lastPlayTime") 
        })
    return player_records

def main():
    logger.info("Starting riot-challenger-etl pipeline...")
    start_time = time.time()

    challenger_entries = get_challenger_ladder()
    if not challenger_entries:
        logger.error("Failed to fetch Challenger ladder. Exiting.")
        return

    champion_mapping = get_champion_mapping()
    raw_mastery_data = []
    
    logger.info(f"Concurrently processing top {len(challenger_entries)} players...")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(get_player_data, player, idx): idx 
            for idx, player in enumerate(challenger_entries, start=1)
        }
        
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    raw_mastery_data.extend(result)
            except Exception as e:
                rank_idx = futures[future]
                logger.error(f"Failed to process player at rank {rank_idx}: {e}")

    logger.info("Player enrichment complete. Transforming data...")
    transformed_df = transform_data(raw_mastery_data)
    transformed_df = add_champion_names(transformed_df, champion_mapping)

    load_to_parquet(transformed_df)

    end_time = time.time()
    logger.info(f"Pipeline completed successfully in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()