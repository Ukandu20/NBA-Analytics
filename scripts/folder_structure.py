#!/usr/bin/env python3
"""
create_nba_folder_structure.py

This script will auto-generate the “nba_analytics_project” folder layout:
- data/external
- data/raw/player_stats/…
- data/raw/team_stats/…
- data/raw/schedule/…
- data/processed/per_team/…
- data/processed/per_team_stats/…
- data/processed/feature_store
"""

import os

def main():
    # 1) Folders under data/external and data/raw
    base_dirs = [
        # Top‐level “external” (for any manually downloaded PDFs, Excel, etc.)
        "data/external",

        # ───────────────── data/raw ─────────────────
        # Raw players & teams CSVs
        "data/raw",
        "data/raw/players_basic.csv",     # (placeholder; we create the folder, not the file)
        "data/raw/players_detailed.csv",
        "data/raw/teams_basic.csv",
        "data/raw/teams_detailed.csv",
        "data/raw/mvp_raw.csv",
        "data/raw/all_players_raw.csv",

        # ─ player_stats categories ─────────────────
        "data/raw/player_stats/clutch",  # clutch stats
        "data/raw/player_stats/defense_dashboard",
        "data/raw/player_stats/shot_dashboard",
        "data/raw/player_stats/box_scores",
        "data/raw/player_stats/adv_box_scores",
        "data/raw/player_stats/shooting",
        "data/raw/player_stats/opponent_shooting",  # opponent shooting stats
        "data/raw/player_stats/general",
        "data/raw/player_stats/playtype",
        "data/raw/player_stats/awards",

        # -player_stats subfolders ────────────────
        # General subfolders
        "data/raw/player_stats/general/traditional",
        "data/raw/player_stats/general/advanced",
        "data/raw/player_stats/general/misc",
        "data/raw/player_stats/general/scoring",
        "data/raw/player_stats/general/defense",
        "data/raw/player_stats/general/opponent",
        "data/raw/player_stats/general/usage",

        # Playtype subfolders
        "data/raw/player_stats/playtype/isolation",
        "data/raw/player_stats/playtype/post_up",
        "data/raw/player_stats/playtype/transition",
        "data/raw/player_stats/playtype/spot_up",
        "data/raw/player_stats/playtype/pick_and_roll_ball_handler",
        "data/raw/player_stats/playtype/pick_and_roll_man",
        "data/raw/player_stats/playtype/hand_off",
        "data/raw/player_stats/playtype/cuts",
        "data/raw/player_stats/playtype/off_screen",
        "data/raw/player_stats/playtype/putbacks",
        "data/raw/player_stats/playtype/misc",
        # Defense Dashboard subfolders
        "data/raw/player_stats/defense_dashboard/overall",
        "data/raw/player_stats/defense_dashboard/3_pointers",
        "data/raw/player_stats/defense_dashboard/2_pointers",
        "data/raw/player_stats/defense_dashboard/below_6ft",
        "data/raw/player_stats/defense_dashboard/6ft_to_10ft",
        "data/raw/player_stats/defense_dashboard/10ft_to_16ft",
        "data/raw/player_stats/defense_dashboard/16ft_to_3pt",   
        
        # Clutch subfolders
        "data/raw/player_stats/clutch/traditional",
        "data/raw/player_stats/clutch/advanced",
        "data/raw/player_stats/clutch/misc",
        "data/raw/player_stats/clutch/scoring",
        "data/raw/player_stats/clutch/opponent",
        "data/raw/player_stats/clutch/four_factors",
        # Shot Dashboard subfolder
        "data/raw/player_stats/shot_dashboard/general",
        "data/raw/player_stats/shot_dashboard/shotclock",
        "data/raw/player_stats/shot_dashboard/dribbles",
        "data/raw/player_stats/shot_dashboard/touch_time",
        "data/raw/player_stats/shot_dashboard/closest_defender",
        "data/raw/player_stats/shot_dashboard/closest_defender_10ft",
        
        # Advanced Box Scores subfolders
        "data/raw/player_stats/adv_box_scores/advanced",
        "data/raw/player_stats/adv_box_scores/traditional",
        "data/raw/player_stats/adv_box_scores/scoring",
        "data/raw/player_stats/adv_box_scores/misc",
        "data/raw/player_stats/adv_box_scores/four_factors",
        # Shooting (distance ranges) subfolders
        "data/raw/player_stats/shooting/distance_range/by_zone",
        "data/raw/player_stats/shooting/distance_range/5ft",
        "data/raw/player_stats/shooting/distance_range/8ft",
        # Opponent Shooting subfolders
        "data/raw/player_stats/opponent_shooting/overall",
        "data/raw/player_stats/opponent_shooting/general",
        "data/raw/player_stats/opponent_shooting/shotclock",
        "data/raw/player_stats/opponent_shooting/dribbles",
        "data/raw/player_stats/opponent_shooting/touch_time",
        "data/raw/player_stats/opponent_shooting/closest_defender",
        "data/raw/player_stats/opponent_shooting/closest_defender_10ft",
        

        # ─ team_stats categories ────────────────────
        "data/raw/team_stats/general",
        "data/raw/team_stats/clutch",
        "data/raw/team_stats/advanced",
        "data/raw/team_stats/playtype",
        "data/raw/team_stats/defense_dashboard",
        "data/raw/team_stats/shot_dashboard",
        "data/raw/team_stats/box_scores",
        "data/raw/team_stats/adv_box_scores",
        "data/raw/team_stats/shooting",
        "data/raw/team_stats/opponent_shooting",   

        # -team_stats subfolders ─────────────────
        # General subfolders
        "data/raw/team_stats/general/traditional",
        "data/raw/team_stats/general/advanced",  
        "data/raw/team_stats/general/misc",
        "data/raw/team_stats/general/scoring",
        "data/raw/team_stats/general/defense",
        "data/raw/team_stats/general/opponent",
        "data/raw/team_stats/general/four_factors",
        # Playtype subfolders
        "data/raw/team_stats/playtype/isolation",
        "data/raw/team_stats/playtype/post_up",
        "data/raw/team_stats/playtype/transition",
        "data/raw/team_stats/playtype/spot_up",
        "data/raw/team_stats/playtype/pick_and_roll_ball_handler",
        "data/raw/team_stats/playtype/pick_and_roll_man",
        "data/raw/team_stats/playtype/hand_off",
        "data/raw/team_stats/playtype/cuts",
        "data/raw/team_stats/playtype/off_screen",
        "data/raw/team_stats/playtype/putbacks",
        "data/raw/team_stats/playtype/misc",
        # Defense Dashboard subfolders
        "data/raw/team_stats/defense_dashboard/overall",
        "data/raw/team_stats/defense_dashboard/3_pointers",
        "data/raw/team_stats/defense_dashboard/2_pointers",
        "data/raw/team_stats/defense_dashboard/below_6ft",
        "data/raw/team_stats/defense_dashboard/6ft_to_10ft",
        "data/raw/team_stats/defense_dashboard/10ft_to_16ft",
        "data/raw/team_stats/defense_dashboard/16ft_to_3pt",   
        
        # Clutch subfolders
        "data/raw/team_stats/clutch/traditional",
        "data/raw/team_stats/clutch/advanced",
        "data/raw/team_stats/clutch/misc",
        "data/raw/team_stats/clutch/scoring",
        "data/raw/team_stats/clutch/opponent",
        "data/raw/team_stats/clutch/four_factors",
        # Shot Dashboard subfolder
        "data/raw/team_stats/shot_dashboard/general",
        "data/raw/team_stats/shot_dashboard/shotclock",
        "data/raw/team_stats/shot_dashboard/dribbles",
        "data/raw/team_stats/shot_dashboard/touch_time",
        "data/raw/team_stats/shot_dashboard/closest_defender",
        "data/raw/team_stats/shot_dashboard/closest_defender_10ft",
        
        # Advanced Box Scores subfolders
        "data/raw/team_stats/adv_box_scores/advanced",
        "data/raw/team_stats/adv_box_scores/traditional",
        "data/raw/team_stats/adv_box_scores/scoring",
        "data/raw/team_stats/adv_box_scores/misc",
        "data/raw/team_stats/adv_box_scores/four_factors",
        # Shooting (distance ranges) subfolders
        "data/raw/team_stats/shooting/distance_range/by_zone",
        "data/raw/team_stats/shooting/distance_range/5ft",
        "data/raw/team_stats/shooting/distance_range/8ft",
        # Opponent Shooting subfolders
        "data/raw/team_stats/opponent_shooting/overall",
        "data/raw/team_stats/opponent_shooting/general",
        "data/raw/team_stats/opponent_shooting/shotclock",
        "data/raw/team_stats/opponent_shooting/dribbles",
        "data/raw/team_stats/opponent_shooting/touch_time",
        "data/raw/team_stats/opponent_shooting/closest_defender",
        "data/raw/team_stats/opponent_shooting/closest_defender_10ft",


        # ─ schedule folders by year ───────────────────
        "data/raw/schedule/2019",
        "data/raw/schedule/2020",
        "data/raw/schedule/2021",
        "data/raw/schedule/2022",
        "data/raw/schedule/2023",
        "data/raw/schedule/2024",
        "data/raw/schedule/2025",

        # ───────────────── data/processed ─────────────────
        "data/processed",
        "data/processed/all_players_cleaned.csv",  # (placeholder path)
        "data/processed/all_teams_cleaned.csv",    # (optional)
        "data/processed/per_team",                 # top‐level “per_team” parent
        "data/processed/per_team_stats",           # top‐level “per_team_stats” parent
        "data/processed/feature_store",
    ]

    # 2) Generate per-team subfolders under data/processed/per_team and per_team_stats
    #    We’ll create a (hard‐coded) list of all 30 NBA team abbreviations + FA + RET
    team_codes = [
        "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW",
        "HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK",
        "OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS",
        "FA","RET"
    ]
    for code in team_codes:
        # Under data/processed/per_team/{TEAM}/
        base_dirs.append(f"data/processed/per_team/{code}")
        # Under data/processed/per_team_stats/{TEAM}/
        base_dirs.append(f"data/processed/per_team_stats/{code}")
        # We can also create a couple of season subfolders (e.g. 2024, 2025) as placeholders
        base_dirs.append(f"data/processed/per_team_stats/{code}/2024")
        base_dirs.append(f"data/processed/per_team_stats/{code}/2025")

    # 3) Actually create all directories
    for d in base_dirs:
        # We only want the folder part; if the string ends with “.csv” it’s just a placeholder.
        # os.makedirs ignores trailing slash, so it’s safe either way.
        dir_to_make = d
        if d.endswith(".csv"):
            dir_to_make = os.path.dirname(d)
        os.makedirs(dir_to_make, exist_ok=True)

    print("✅ All folders have been created or already existed.")


if __name__ == "__main__":
    main()
