from __future__ import annotations
import streamlit as st


def render_charts(dfs: dict, kpis: dict, season: str, season_type: str, team_abbr: str, team_name: str, prev_season: str | None) -> None:
    """Render all charts and metrics for the team dashboard."""
    locals().update(dfs)
    locals().update(kpis)
    container = st.container(border=True)
    with container:
        container.markdown(
            f"""
            <div style="display: flex; align-items: center; justify-content: space-between;">
            <h1 style="margin: 0;">How good were the {team_name} in {season}?</h1>
            <img src="{logo_url}" alt="{team_abbr} logo" style="height:50px">
            </div>
            """,
            unsafe_allow_html=True
        )
        col1, col2, col3, col4, col5= container.columns(5)

        delta_win = f"{win_arrow}{win_delta}% from {prev_season} and #{win_loss_pct_rank}" if win_delta or win_loss_pct_rank else None
        info_win = "The percentage of games played that a team has won"
        col1.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Win %
                <span
                title="{info_win}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {win_loss_pct:.1f}
                </div>
                <div style="font-size: 0.9rem; color: {win_color}; font-weight: 500; margin-top: 0.25rem;">
                {win_arrow}{win_delta}% from {prev_season}, <span style="color: #28a745">#{win_loss_pct_rank} of {total_teams}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        #Net Rating
        delta_text = f"{net_rating_rank} in the league" if net_rating_rank else None
                #offensive Rating
        delta_off = f"{off_delta} from {prev_season}" if off_delta else None
        info_off = "The total number of points scored per 100 possessions"
        col2.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Offensive Rating
                <span
                title="{info_off}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {off_rating:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{off_rating_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        #Defensive Rating
        delta_def = f"{def_delta} from {prev_season}" if def_delta else None
        info_def = "The total number of points allowed per 100 possessions"
        col3.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Defensive Rating
                <span
                title="{info_def}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {def_rating:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{def_rating_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        info_net = "Team’s point differential per 100 possessions"
        col4.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Net Rating
                <span
                title="{info_net}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {net_rating:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{net_rating_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        
        # True Shooting Percentage (TS%)
        info_ts = "An efficiency metric that weights three-point shots and free throws for their extra value, alongside traditional two-point field goals"
        col5.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                True Shooting %
                <span
                title="{info_ts}"
                style="margin-left: 0.5rem; cursor: pointer; color: #888;"
                >ℹ️</span>
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 3rem; font-weight: bold; color: #0a7dfa; line-height: 1;">
                {ts_pct:.1f}
                </div>
                <div style="font-size: 0.9rem; color: #28a745; font-weight: 500; margin-top: 0.25rem;">
                #{ts_pct_rank} of {total_teams} 
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)


        # ─────────────────────────────────────────────────────────────────────────────
        # Additional team-level KPIs
        # ─────────────────────────────────────────────────────────────────────────────
        

        col1, col2, col3, col4 = st.columns([2,2,1,1])
        col1.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Biggest win
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="color:green">{largest_victory_pts} - {victory_opp_points}</span> {vic_prefix} {largest_victory_opponent}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)
        
        col2.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Heaviest Defeat
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="color:red">{biggest_defeat_pts} - {defeat_opp_points}</span> {def_prefix} {biggest_defeat_opponent}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        col3.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Home Record
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="font-size:2rem;">{h_wins}-{h_losses}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        col4.markdown(f"""
            <div style="
                border: 1px solid #555;
                border-radius: 8px;
                padding: 0.8rem;
                padding-left:1rem;
                max-height: 124.5px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                margin-top:1rem;
                margin-bottom:1rem;
            ">
            <!-- Title + info icon -->
            <div style="
                display: flex;
                align-items: center;
                font-size: clamp(0.75rem, 2.5vw, 0.9rem);
                color: #ddd;
                margin-bottom: 0.02rem;
            ">
                Away Record
            </div>

            <!-- Value + delta -->
            <div>
                <div style="font-size: 2rem; font-weight: bold; color: #ddd; line-height: 1;">
                <span style="font-size:2rem;">{a_wins}-{a_losses}</span>
                </div>
            </div>
            </div>
            """, unsafe_allow_html=True)

        # scale headshots down 12× then 1.5×
        orig_w, orig_h = 1040, 760
        new_w = round(orig_w / (12 * 1.5))
        new_h = round(orig_h / (12 * 1.5))

        # define titles, suffixes and their dfs
        stat_info = [
            ("Top 3 Scorers", "PPG", "ppg", top3_pts),
            ("Rebound Leaders", "RPG", "rpg", top3_reb),
            ("Assist Leaders", "APG", "apg", top3_ast),
        ]

        cols = st.columns(3)
        for col, (title, suffix, colname, df_lead) in zip(cols, stat_info):
            with col:
                # outer KPI-style box
                html = f'''
                <div style="
                    border:1px solid #555;
                    border-radius:8px;
                    padding:1rem;
                    box-sizing:border-box;
                    margin-bottom:1rem;
                ">
                <h4 style="margin:0 0 0.75rem 0; color:#fff;">{title}</h4>
                '''
                # each of the top 3 players
                for i, row in df_lead.iterrows():
                    html += f'''
                <div style="
                    display:flex;
                    align-items:center;
                    justify-content:space-between;
                    margin-bottom:0.75rem;
                ">
                    <div style="display:flex; align-items:center;">
                    <img src="{row['headshot_url']}"
                        style="
                            border-radius:50%;
                            width:{new_w}px;
                            height:{new_h}px;
                            object-fit:cover;
                            margin-right:0.75rem;
                        ">
                    <span style="font-weight:600; color:#fff;">
                        {i+1}. {row['player']}
                    </span>
                    </div>
                    <span style="font-weight:700; color:#0a7dfa;">
                    {row[colname]:.1f} {suffix}
                    </span>
                </div>
                '''
                html += "</div>"
                st.markdown(html, unsafe_allow_html=True)

                
                


        col1, col2= st.columns([1,1])
        with col1:
            if {"pts", "reb", "ast", "game_date"}.issubset(df_team_metric.columns):
                # 1) Prepare the data
                df = df_team_metric.copy()
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                df["month_num"]  = df["game_date"].dt.month

                by_month = (
                    df
                    .groupby("month_num")
                    .agg(
                        PPG=("pts", "mean"),
                        RPG=("reb", "mean"),
                        APG=("ast", "mean"),
                    )
                    .round(2)
                    .reset_index()  # month_num column retained
                )

                # Preserve season order via earliest date per month
                order = (
                    df
                    .groupby("month_num")["game_date"]
                    .min()
                    .sort_values()
                    .index
                    .tolist()
                )

                # Map to labels and define x-axis domain
                by_month["month"] = by_month["month_num"].map(months)
                x_domain = [months[m] for m in order]

                # Melt into long form for grouped bars
                df_long = by_month.melt(
                    id_vars=["month"],
                    value_vars=["PPG", "RPG", "APG"],
                    var_name="Metric",
                    value_name="Value",
                )

                # 2) Compute league-wide medians (replace df_gen if needed)
                medians = {
                    "PPG": df_gen["pts"].median(),
                    "RPG": df_gen["reb"].median(),
                    "APG": df_gen["ast"].median(),
                }

                df_plot = df_long.copy()
                df_plot["median"] = df_plot["Metric"].map(medians)
                df_plot["Diff"]   = df_plot["Value"] - df_plot["median"]

                # flag the max/min Diff _within each_ Metric
                df_plot["is_max"] = (
                    df_plot.groupby("Metric")["Diff"]
                        .transform("max") == df_plot["Diff"]
                )
                df_plot["is_min"] = (
                    df_plot.groupby("Metric")["Diff"]
                        .transform("min") == df_plot["Diff"]
                )


                        # ——— 2) Compute best/worst PPG & league median ———
                best_idx   = by_month["PPG"].idxmax()
                worst_idx  = by_month["PPG"].idxmin()
                best_row   = by_month.loc[best_idx]
                worst_row  = by_month.loc[worst_idx]

                best_month   = best_row["month"]
                best_points  = best_row["PPG"]
                worst_month  = worst_row["month"]
                worst_points = worst_row["PPG"]

                best_ast_idx   = by_month["APG"].idxmax()
                worst_ast_idx  = by_month["APG"].idxmin()
                best_ast_row   = by_month.loc[best_ast_idx]
                worst_ast_row  = by_month.loc[worst_ast_idx]

                best_ast_month   = best_ast_row["month"]
                best_ast  = best_ast_row["APG"]
                worst_ast_month  = worst_ast_row["month"]
                worst_ast = worst_ast_row["APG"]

                best_reb_idx   = by_month["RPG"].idxmax()
                worst_reb_idx  = by_month["RPG"].idxmin()
                best_reb_row   = by_month.loc[best_reb_idx]
                worst_reb_row  = by_month.loc[worst_reb_idx]

                best_reb_month   = best_reb_row["month"]
                best_reb  = best_reb_row["RPG"]
                worst_reb_month  = worst_reb_row["month"]
                worst_reb = worst_reb_row["RPG"]

                # 3) Build the bar chart
                bars = alt.Chart(df_long).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain, title="Month"),
                    xOffset="Metric:N",
                    y=alt.Y("Value:Q", title="Avg per Game"),
                    color=alt.Color(
                        "Metric:N",
                        scale=alt.Scale(
                            domain=["PPG", "RPG", "APG"],
                            range=["#83c9ff", "#ffb366", "#a3a3ff"]
                        ),
                        legend=alt.Legend(title="Metric")
                    )
                )

                                # ——— 4) Highlight best/worst per metric ———
                highlight_max = alt.Chart(df_plot[df_plot["is_max"]]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    xOffset="Metric:N",
                    y="Value:Q",
                    color=alt.value("#7defa1")
                )
                highlight_min = alt.Chart(df_plot[df_plot["is_min"]]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    xOffset="Metric:N",
                    y="Value:Q",
                    color=alt.value("#ff2b2b")
                )

                # 4) Create median reference lines + labels
                rules = [
                    alt.Chart(pd.DataFrame({"median": [med]}))
                    .mark_rule(strokeDash=[4,4], stroke="gray")
                    .encode(y="median:Q", detail="Metric:N")
                    for m, med in medians.items()
                ]
                texts = [
                    alt.Chart(pd.DataFrame({"median": [med]}))
                    .mark_text(dx=3, dy=-5, color="gray")
                    .encode(y="median:Q", text=alt.value(f"{m} med → {med:.1f}"))
                    for m, med in medians.items()
                ]

                # 5) Layer everything and render
                chart = alt.layer(bars, highlight_max, highlight_min, *rules, *texts).properties(
                    title="Monthly PPG / RPG / APG"
                )

                st.altair_chart(chart, use_container_width=True) # type: ignore[arg-type]
                st.markdown(
                    f"""
                    <div style="
                        font-size: 0.875rem;
                        color: #888;
                        line-height: 1.4;
                        margin-top: 0.2rem;
                        padding: 0 0;
                    ">
                    The best month was <strong>{best_month}</strong> with <strong>{best_points}</strong> pts → (green bar).<br>
                    The worst month was <strong>{worst_month}</strong> with <strong>{worst_points}</strong> pts → (red bar).<br>

                    </div>
                    """,
                    unsafe_allow_html=True
                )





        #Point differential per month
        with col2:
            if "plus_minus" in df_team_metric.columns:
                # 1) Prepare the data
                df = df_team_metric.copy()
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                diff_by_month = (
                    df
                    .groupby(df["game_date"].dt.month)["net_rating"]
                    .mean()
                    .round(1)
                    .reset_index(name="Diff")
                )
                # preserve season order (Oct→Apr)
                order = (
                    df
                    .groupby(df["game_date"].dt.month)["game_date"]
                    .min()
                    .sort_values()
                    .index
                )
                diff_by_month["month"] = diff_by_month["game_date"].map(lambda m: months[m])
                x_domain = [months[m] for m in order]

                # identify best/worst
                max_diff = diff_by_month["Diff"].max()
                min_diff = diff_by_month["Diff"].min()

                best_idx = diff_by_month["Diff"].idxmax()
                worst_idx = diff_by_month["Diff"].idxmin()
                best_month = diff_by_month.loc[best_idx, "month"]
                best_value = diff_by_month.loc[best_idx, "Diff"]
                worst_month = diff_by_month.loc[worst_idx, "month"]
                worst_value = diff_by_month.loc[worst_idx, "Diff"]

                median_diff = df["net_rating"].median()

                # 2) Base bars (light gray)
                base = alt.Chart(diff_by_month).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain, title="Month"),
                    y=alt.Y("Diff:Q", title="Avg Margin"),
                    color=alt.value("#83c9ff")
                )
                # 3) Highlight best (green) & worst (red)
                highlight_max = alt.Chart(diff_by_month[diff_by_month.Diff == max_diff]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="Diff:Q",
                    color=alt.value("#7defa1")
                )
                highlight_min = alt.Chart(diff_by_month[diff_by_month.Diff == min_diff]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="Diff:Q",
                    color=alt.value("#ff2b2b")
                )
                # 4) Median rule + label
                median_rule = alt.Chart(pd.DataFrame({"Diff":[median_diff]})).mark_rule(
                    color="gray", strokeDash=[4,4]
                ).encode(y="Diff:Q")
                median_text = alt.Chart(pd.DataFrame({"Diff":[median_diff]})).mark_text(
                    align="left", dx=3, dy=-5, color="gray"
                ).encode(
                    y="Diff:Q",
                )

                chart = (
                    base
                    + highlight_max
                    + highlight_min
                    + median_rule
                    + median_text
                ).properties(
                    title="Monthly Net Rating",
                    width="container"
                )

                st.altair_chart(chart, use_container_width=True) # type: ignore[arg-type] 

                # caption with real line breaks
                st.markdown(
                    f"""
                    <div style="
                        font-size: 0.875rem;
                        color: #888;
                        line-height: 1.4;
                        margin-top: 0.2rem;
                        margin-bottom: 2rem;
                        padding: 0 0;
                    ">
                    The best month was <strong>{best_month}</strong> with <strong>{best_value}</strong> pts → (green bar).<br>
                    The worst month was <strong>{worst_month}</strong> with <strong>{worst_value}</strong> pts → (red bar).<br>
                    The league median is {median_diff:.1f}pts → (dashed line).
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        # ─────────────────────────────────────────────────────────────────────────────
        # PPG per Month chart
        # ─────────────────────────────────────────────────────────────────────────────

        # ────────────────────────────────────────────────────────────────────────────
        # Advanced Stats KPIs
        # ────────────────────────────────────────────────────────────────────────────
        c9, c10= st.columns([1,1])

        #Shooting Efficiency Group bar Chart containing eFG%, TS%, FT% and 3PT%
        with c9:
            # 1) Read in and compute league‐wide medians
            df_trad_all = (
                pd.read_csv(gen_trad)
                .rename(columns=str.lower)
                .drop(columns=["team"], errors="ignore")
            )
            df_adv_all = (
                pd.read_csv(clutch_adv)
                .rename(columns=str.lower)
                .drop(columns=["team"], errors="ignore")
            )
            median_vals = {
                "efg_pct": df_adv_all["efg_pct"].median(),
                "ts_pct":  df_adv_all["ts_pct"].median(),
                "ft_pct":  df_trad_all["ft_pct"].median(),
                "fg3_pct": df_trad_all["fg3_pct"].median(),
            }

            # 2) Pull your team’s shooting metrics and melt to long form
            shoot_eff = (
                df_team_gen_trad[["team","team_id","ft_pct","fg3_pct"]]
                .merge(df_clutch_adv[["team_id","efg_pct","ts_pct"]], on="team_id")
            )
            metrics = ["efg_pct","ts_pct","ft_pct","fg3_pct"]
            df_long = shoot_eff.melt(
                id_vars=["team"],
                value_vars=metrics,
                var_name="Metric",
                value_name="Pct"
            )

            # 3) Build grouped bars with xOffset
            color_scale = alt.Scale(
                domain=metrics,
                range=["#83c9ff","#ffb366","#7defa1","#ff7f7f"]
            )
            bars = (
                alt.Chart(df_long)
                .mark_bar(size=40)
                .encode(
                    x=alt.X("team:N", title="Team"),
                    xOffset="Metric:N",
                    y=alt.Y("Pct:Q", title="Shooting %"),
                    color=alt.Color("Metric:N", scale=color_scale, legend=alt.Legend(title="Metric"))
                )
            )

            # 4) Add dashed‐gray median lines & labels for each metric
            rules = []
            texts = []
            for m in metrics:
                med = median_vals[m]
                df_med = pd.DataFrame({"median":[med]})
                rules.append(
                    alt.Chart(df_med)
                    .mark_rule(color="gray", strokeDash=[4,4])
                    .encode(y="median:Q")
                )
                texts.append(
                    alt.Chart(df_med)
                    .mark_text(dx=3, dy=-5, color="gray")
                    .encode(
                        y="median:Q",
                        text=alt.value(f"{m} med → {med:.1f}%")
                    )
                )

            chart = (bars).properties(
                title="Shooting Efficiency by Metric",
                width="container",
            )

            st.altair_chart(chart, use_container_width=True)

            # 5) Caption explaining colors & medians
            st.markdown(
                """
                <div style="
                    font-size:0.875rem;
                    color:#888;
                    line-height:1.4;
                    margin-top:0.5rem;
                    margin-bottom:2rem;
                ">
                    <strong>Blue</strong> = eFG% • <strong>Orange</strong> = TS% • 
                    <strong>Green</strong> = FT% • <strong>Red</strong> = 3P%<br>
                    Dashed gray lines = league medians
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ────────────────────────────────────────────────────────────────────────────
        # Rebounding and Hustle Stats KPIs
        with c10:
            # ── Prep hustle metrics ────────────────────────────────
            df_team_gen_adv = df_team_gen_adv.merge(
                df_team_gen_trad[["team_id","blka", "blk", "stl"]],
                on="team_id", how="left"
            )
            df_team_gen_adv["blk_pct"] = (df_team_gen_adv["blk"] / df_team_gen_adv["blka"]).fillna(0)

            # get medians
            metrics = ["dreb_pct", "oreb_pct", "stl", "blk_pct"]
            median_vals = {m: df_team_gen_adv[m].median() for m in metrics}

            # melt into long form
            df_hustle = (
                df_team_gen_adv[["team"] + metrics]
                .melt(id_vars="team",
                    value_vars=metrics,
                    var_name="Hustle Metric",
                    value_name="Percentage")
            )

            # ── Base bars ───────────────────────────────────────────
            color_scale = alt.Scale(
                domain=metrics,
                range=["#83c9ff","#ffb366","#7defa1","#ff7f7f"]
            )
            bars = (
                alt.Chart(df_hustle)
                .mark_bar(size=40)
                .encode(
                    x=alt.X("team:N", title="Team"),
                    xOffset=alt.XOffset("Hustle Metric:N"),
                    y=alt.Y("Percentage:Q", title="Value"),
                    color=alt.Color("Hustle Metric:N",
                                    scale=color_scale,
                                    legend=alt.Legend(title="Metric"))
                )
            )

            # ── Median lines & labels ───────────────────────────────
            layers = [bars]
            for m in metrics:
                med = median_vals[m]
                df_med = pd.DataFrame({"median":[med]})
                # rule
                layers.append(
                    alt.Chart(df_med)
                    .mark_rule(color="gray", strokeDash=[4,4])
                    .encode(y="median:Q")
                )
                # label
                layers.append(
                    alt.Chart(df_med)
                    .mark_text(dx=3, dy=-5, color="gray")
                    .encode(
                        y="median:Q",
                        text=alt.value(f"{m} median → {med:.2f}")
                    )
                )

            # ── Compose & render ─────────────────────────────────────
            chart = (
                alt.layer(*layers)
                .properties(
                    title="Rebounding & Hustle Metrics",
                    width="container",
                )
            )

            st.altair_chart(chart, use_container_width=True) #type: ignore[arg-type]


        radar1, radar2 = st.columns([1,1])
        with radar2:
            # 1) build df_eff
            labels = list(eff_benchmark_metrics.keys())
            cols   = list(eff_benchmark_metrics.values())
            team_pct = [ league_percentile.loc[team_id, c] for c in cols ]
            df_eff = pd.DataFrame({
                "Metric": labels,
                "Value":  [round(p * 100, 1) for p in team_pct]
            })
            median_val = 50
            # add the combined text label
            df_eff["label"] = df_eff.apply(lambda r: f"{r.Metric}: {r.Value:.1f}", axis=1)

            scale = alt.Scale(domain=[0,100], rangeMin=20, rangeMax=120)

            median_ring = (
                alt.Chart(pd.DataFrame({"m":[median_val]}))
                .mark_arc(stroke="gray", strokeDash=[4,4], fillOpacity=0)
                .encode(
                    theta=alt.value(2*math.pi),
                    radius=alt.Radius("m:Q", scale=scale)
            ))
            above = df_eff[df_eff.Value >= median_val]
            below = df_eff[df_eff.Value <  median_val]

            bars_above = (
                alt.Chart(above)
                .mark_arc(innerRadius=30, stroke="#fff")
                .encode(
                    theta=alt.Theta("Metric:N", sort=labels, title=None),
                    radius=alt.Radius("Value:Q", scale=scale),
                    color=alt.value("green")
            ))
            bars_below = (
                alt.Chart(below)
                .mark_arc(innerRadius=30, stroke="#fff")
                .encode(
                    theta=alt.Theta("Metric:N", sort=labels, title=None),
                    radius=alt.Radius("Value:Q", scale=scale),
                    color=alt.value("red")
            ))
            # now use the new label field
            labels_layer = (
                alt.Chart(df_eff)
                .mark_text(radiusOffset=30, fontSize=12)
                .encode(
                    theta=alt.Theta("Metric:N", sort=labels),
                    radius=alt.Radius("Value:Q", scale=scale),
                    text=alt.Text("label:N"),
                    color=alt.value("white")
            ))

            radar = alt.layer(
                median_ring,
                bars_below, bars_above,
                labels_layer
            ).properties(
                width="container",
                title=f"Efficiency Radar — {team_name}"
            ).configure_view(stroke=None)

            st.altair_chart(radar, use_container_width=True) #type: ignore[arg-type]
            st.markdown(
                """
                <div style="
                    font-size:0.875rem;
                    color:#888;
                    line-height:1.4;
                    margin-top:0.5rem;
                    margin-bottom:2rem;
                ">
                    <strong>Blue</strong> = eFG% • <strong>Orange</strong> = TS% • 
                    <strong>Green</strong> = FT% • <strong>Red</strong> = 3P%<br>
                    Dashed gray lines = league medians
                </div>
                """,
                unsafe_allow_html=True,
            )


        with radar1:

            ft_pct = df_team_gen_scoring["pct_pts_ft"].iloc[0]
            fg2_pct = df_team_gen_scoring["pct_pts_2pt"].iloc[0]
            fg3_pct = df_team_gen_scoring["pct_pts_3pt"].iloc[0]
            pts = df_team_gen_scoring["pts"].iloc[0]

            ft_ppg  = round((ft_pct * pts), 1)
            fg2_ppg = round((fg2_pct * pts), 1)
            fg3_ppg = round((fg3_pct * pts), 1)

            # 2) Build a DataFrame of shot-type → ppg → share
            df_shot = pd.DataFrame({
                "Shot Type": ["Free Throws", "Two-Pointers", "Three-Pointers"],
                "PPG":        [ft_ppg, fg2_ppg, fg3_ppg],
            })
            #df_shot_share["Share"] = df_shot_share["PPG"] / df_shot_share["PPG"].sum()

                    # 2) Build a little helper array for the θ‐ordering:
            order = ["Free Throws", "Two-Pointers", "Three-Pointers"]

             # define a common scale so arcs and text share the exact same colors
            color_scale = alt.Scale(scheme="category10")

            # 1) Arc layer
            arcs = (
                alt.Chart(df_shot)
                .mark_arc(innerRadius=40, stroke="white")
                .encode(
                    theta=alt.Theta("Shot Type:N", sort=order, title=None),
                    radius=alt.Radius("PPG:Q",
                                    scale=alt.Scale(type="sqrt", zero=True, rangeMin=20),
                                    title="PPG"),
                    color=alt.Color("Shot Type:N",
                                    scale=color_scale,
                                    legend=alt.Legend(title="Shot Type")),
                    tooltip=[
                        alt.Tooltip("Shot Type:N", title="Shot Type"),
                        alt.Tooltip("PPG:Q", format=".1f", title="PPG")
                    ],
                )
            )

            # 2) Text layer, color‐mapped by the same "Shot Type"
            labels = (
                alt.Chart(df_shot)
                .mark_text(radiusOffset=20, fontSize=13)
                .encode(
                    theta=alt.Theta("Shot Type:N", sort=order),
                    radius=alt.Radius("PPG:Q",
                                    scale=alt.Scale(type="sqrt", zero=True, rangeMin=20)),
                    text=alt.Text("PPG:Q", format=".1f"),
                    color=alt.Color("Shot Type:N", scale=color_scale, legend=None)
                )
            )

            # 3) layer them
            chart = (arcs + labels).properties(
                title=f"{team_abbr} Shot-Type Breakdown (PPG share)",
                width="container"
            )


            st.altair_chart(chart, use_container_width=True) #type: ignore[arg-type]