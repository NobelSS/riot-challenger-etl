import time
from src.logger import get_logger
from src.extract import get_challenger_ladder, get_summoner_name_by_puuid, get_top_champion_masteries, get_champion_mapping
from src.transform import transform_data, add_champion_names
from src.load import load_to_parquet

logger = get_logger(__name__)

def main():
    logger.info("Starting riot-challenger-etl pipeline...")
    start_time = time.time()

    challenger_entries = get_challenger_ladder()
    if not challenger_entries:
        logger.error("Failed to fetch Challenger ladder. Exiting.")
        return

    champion_mapping = get_champion_mapping()

    raw_mastery_data = []
    total_players = len(challenger_entries)
    
    logger.info(f"Processing top {total_players} players...")

    for rank_idx, player in enumerate(challenger_entries, start=1):
        puuid = player.get("puuid")
        
        if not puuid:
            logger.warning(f"Could not resolve PUUID for rank {rank_idx}. Skipping.")
            continue

        summoner_name = get_summoner_name_by_puuid(puuid)
        if not summoner_name:
            logger.warning(f"Could not resolve name for rank {rank_idx}. Skipping.")
            continue

        masteries = get_top_champion_masteries(puuid, count=3)
        
        for mastery in masteries:
            raw_record = {
                "summoner_name": summoner_name,
                "ladder_rank": rank_idx,
                "champion_id": mastery.get("championId"),
                "mastery_level": mastery.get("championLevel"),
                "total_mastery_points": mastery.get("championPoints"),
                "last_played_ms": mastery.get("lastPlayTime") 
            }
            raw_mastery_data.append(raw_record)
            
        if rank_idx % 10 == 0:
            logger.info(f"Extracted {rank_idx}/{total_players} players...")

    transformed_df = transform_data(raw_mastery_data)
    
    transformed_df = add_champion_names(transformed_df, champion_mapping)

    load_to_parquet(transformed_df)

    end_time = time.time()
    logger.info(f"Pipeline completed successfully in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()