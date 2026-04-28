import pandas as pd
from pydantic import BaseModel, ValidationError, Field
from datetime import datetime, timezone
from typing import List, Dict, Any
from src.logger import get_logger

logger = get_logger(__name__)

class ChallengerMastery(BaseModel):
    summoner_name: str
    ladder_rank: int = Field(ge=1)
    champion_id: int
    mastery_level: int
    total_mastery_points: int = Field(ge=0)
    last_played_utc: datetime

def transform_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    logger.info("Transforming and validating data...")
    validated_records = []
    
    for record in raw_data:
        try:
            if "last_played_ms" in record:
                record["last_played_utc"] = datetime.fromtimestamp(
                    record["last_played_ms"] / 1000.0, tz=timezone.utc
                )
            
            validated_record = ChallengerMastery(**record)
            validated_records.append(validated_record.model_dump())
            
        except ValidationError as e:
            logger.error(f"Validation error for record {record.get('summoner_name', 'UNKNOWN')}: {e}")
            continue

    df = pd.DataFrame(validated_records)
    logger.info(f"Successfully transformed {len(df)} rows.")
    return df

def add_champion_names(df: pd.DataFrame, champion_mapping: dict) -> pd.DataFrame:
    if df.empty:
        return df
    
    df['champion_name'] = df['champion_id'].map(champion_mapping)
    
    cols = df.columns.tolist()
    cols.insert(cols.index('champion_id') + 1, cols.pop(cols.index('champion_name')))
    return df[cols]