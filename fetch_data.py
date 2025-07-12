import datetime as dt
from pathlib import Path
import nfl_data_py as nfl

DATA_DIR = Path("data/parquet")
DATA_DIR.mkdir(parents=True, exist_ok=True)

end_date = dt.date.today() - dt.timedelta(days=1)
pbp = nfl.import_pbp_data(years=range(2014, end_date.year + 1))

# Keep a lean set of columns that definitely exist
cols = [
    "game_id", "posteam", "defteam", "week", "season",
    "play_type", "yardline_100", "epa",
    "rush_attempt", "pass_attempt", "complete_pass",
    "yards_gained", "touchdown"
]
pbp[cols].to_parquet(
    DATA_DIR / f"pbp_{end_date}.parquet",
    index=False
)
print("✅  Saved", len(pbp), "rows with", len(cols), "columns")
