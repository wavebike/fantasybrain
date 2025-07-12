"""
Builds a player-week dataset with PPR fantasy points from nflfastR play-by-play.
Run:  python src/build_player_weeks.py
"""

from pathlib import Path
import pandas as pd

PBP_DIR = Path("data/parquet")
OUT_DIR = Path("data/agg")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 1.  load latest parquet ────────────────────────────────────────────────
pbp_file = max(PBP_DIR.glob("pbp_*.parquet"))
pbp      = pd.read_parquet(pbp_file, engine="pyarrow")
print(f"Loaded {len(pbp):,} plays from {pbp_file.name}")

# ── 2.  build three role-specific frames, then stack ───────────────────────
frames   = []

# Rushing actions
mask = pbp["rusher_player_name"].notna()
rush = pbp.loc[mask, ["season","week","posteam",
                      "rusher_player_name","yards_gained","touchdown"]].copy()
rush.rename(columns={"rusher_player_name":"player_name",
                     "yards_gained":"rush_yds",
                     "touchdown":"rush_td"}, inplace=True)
rush["rec_yds"]=rush["pass_yds"]=0
rush["rec_td"]=rush["pass_td"]=0
rush["receptions"]=0
frames.append(rush)

# Receiving actions
mask = pbp["receiver_player_name"].notna()
rec = pbp.loc[mask, ["season","week","posteam",
                     "receiver_player_name","yards_gained","touchdown","complete_pass"]].copy()
rec.rename(columns={"receiver_player_name":"player_name",
                    "yards_gained":"rec_yds",
                    "touchdown":"rec_td",
                    "complete_pass":"receptions"}, inplace=True)
rec["rush_yds"]=rec["pass_yds"]=0
rec["rush_td"]=rec["pass_td"]=0
frames.append(rec)

# Passing actions
mask = pbp["passer_player_name"].notna()
pas = pbp.loc[mask, ["season","week","posteam",
                     "passer_player_name","yards_gained","touchdown"]].copy()
pas.rename(columns={"passer_player_name":"player_name",
                    "yards_gained":"pass_yds",
                    "touchdown":"pass_td"}, inplace=True)
pas["rush_yds"]=pas["rec_yds"]=0
pas["rush_td"]=pas["rec_td"]=0
pas["receptions"]=0
frames.append(pas)

events = pd.concat(frames, ignore_index=True)

# ── 3. aggregate to player-week ────────────────────────────────────────────
stats = {
    "rush_yds"   :"sum",
    "rec_yds"    :"sum",
    "pass_yds"   :"sum",
    "rush_td"    :"sum",
    "rec_td"     :"sum",
    "pass_td"    :"sum",
    "receptions" :"sum"
}
pw = (events
      .groupby(["season","week","posteam","player_name"], observed=True)
      .agg(stats)
      .reset_index())

# ── 4. compute PPR fantasy points ──────────────────────────────────────────
pw["fantasy_points_ppr"] = (
      pw["rush_yds"]/10
    + pw["rec_yds"]/10
    + pw["pass_yds"]/25
    + pw["rush_td"]*6
    + pw["rec_td"]*6
    + pw["pass_td"]*4
    + pw["receptions"]
)

latest_season, latest_week = pw[["season","week"]].max()
print("\nTop-5 PPR last completed week:")
print(pw.query("season==@latest_season and week==@latest_week")
        .nlargest(5, "fantasy_points_ppr")
        [["player_name","fantasy_points_ppr"]])

out_file = OUT_DIR / f"player_weeks_{latest_season}_wk{latest_week}.feather"
pw.to_feather(out_file)
from os import path
rel = path.relpath(out_file, Path.cwd())
print(f"\n✅  Saved {len(pw):,} rows ➜ {rel}")
