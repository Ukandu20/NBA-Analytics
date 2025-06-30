        c_col1, c_col2, c_col3 = st.columns(3)
        with c_col1:
            if "ast" in df_team_metric.columns:
                #1) Prepare the data
                df = df_team_metric.copy()
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                ast_by_month = (
                    df
                .groupby(df["game_date"].dt.month)["ast"]
                    .mean()
                    .round(2)
                        .reset_index(name="APG")
                    )
                    # preserve season order (Oct→Apr)
                order = (
                    df
                    .groupby(df["game_date"].dt.month)["game_date"]
                    .min()
                    .sort_values()
                    .index
                )
                # map numeric month → label
                ast_by_month["month"] = ast_by_month["game_date"].map(lambda m: months[m])
                x_domain = [months[m] for m in order]

                # compute league median for reference
                median_APG = df_gen["ast"].median()

                # identify max/min APG values
                max_APG = ast_by_month["APG"].max()
                min_APG = ast_by_month["APG"].min()

                # now pull out exactly the best & worst months and their APG
                best_idx    = ast_by_month["APG"].idxmax()
                worst_idx   = ast_by_month["APG"].idxmin()
                best_month  = ast_by_month.loc[best_idx,  "month"]
                best_points = ast_by_month.loc[best_idx,  "APG"]
                worst_month = ast_by_month.loc[worst_idx, "month"]
                worst_points= ast_by_month.loc[worst_idx, "APG"]

                # 2) Build the base bars (light blue)
                base = alt.Chart(ast_by_month).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain, title="Month"),
                    y=alt.Y("APG:Q", title="APG"),
                    color=alt.value("#83c9ff")
                )

                # 3) Highlight max & min
                highlight_max = alt.Chart(ast_by_month[ast_by_month.APG == max_APG]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="APG:Q",
                    color=alt.value("#7defa1")
                )
                highlight_min = alt.Chart(ast_by_month[ast_by_month.APG == min_APG]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="APG:Q",
                    color=alt.value("#ff2b2b")
                )

                # 4) Median line + annotation
                median_rule = alt.Chart(pd.DataFrame({"APG":[median_APG]})).mark_rule(
                    color="gray", strokeDash=[4,4]
                ).encode(y="APG:Q")
                median_text = alt.Chart(pd.DataFrame({"APG":[median_APG]})).mark_text(
                    align="left", dx=3, dy=-5, color="gray"
                ).encode(
                    y="APG:Q",
                )

                # 5) Layer everything and render
                chart = (
                    base
                    + highlight_max
                    + highlight_min
                    + median_rule
                    + median_text
                ).properties(
                    title="Monthly Points Per Game (APG)",
                    width="container"
                )
                st.altair_chart(chart, use_container_width=True)  # type: ignore[arg-type]

                st.markdown(
                    f"""
                    <div style="
                        font-size: 0.875rem;
                        color: 888;
                        line-height: 1.4;
                        margin-top: 0.2rem;
                        padding: 0 0;
                    ">
                    The best month was <strong>{best_month}</strong> with <strong>{best_points}</strong>ast → (green bar).<br>
                    The worst month was <strong>{worst_month}</strong> with <strong>{worst_points}</strong>ast → (red bar).<br>
                    The league median is <strong>{median_APG:.1f}</strong>pts → (dashed line).
                    </div>
                    """,
                    unsafe_allow_html=True
                )            

        with c_col2:
            if "pts" in df_team_metric.columns:
                # 1) Prepare the data
                df = df_team_metric.copy()
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                ast_by_month = (
                    df
                    .groupby(df["game_date"].dt.month)["pts"]
                    .mean()
                    .round(2)
                    .reset_index(name="PPG")
                )
                # preserve season order (Oct→Apr)
                order = (
                    df
                    .groupby(df["game_date"].dt.month)["game_date"]
                    .min()
                    .sort_values()
                    .index
                )
                # map numeric month → label
                ast_by_month["month"] = ast_by_month["game_date"].map(lambda m: months[m])
                x_domain = [months[m] for m in order]

                #compute league median for reference
                median_ppg = df_gen["ast"].median()

                # identify max/min PPG values
                max_ppg = ast_by_month["PPG"].max()
                min_ppg = ast_by_month["PPG"].min()

                # now pull out exactly the best & worst months and their PPG
                best_idx    = ast_by_month["PPG"].idxmax()
                worst_idx   = ast_by_month["PPG"].idxmin()
                best_month  = ast_by_month.loc[best_idx,  "month"]
                best_points = ast_by_month.loc[best_idx,  "PPG"]
                worst_month = ast_by_month.loc[worst_idx, "month"]
                worst_points= ast_by_month.loc[worst_idx, "PPG"]

                # 2) Build the base bars (light blue)
                base = alt.Chart(ast_by_month).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain, title="Month"),
                    y=alt.Y("PPG:Q", title="PPG"),
                    color=alt.value("#83c9ff")
                )

                # 3) Highlight max & min
                highlight_max = alt.Chart(ast_by_month[ast_by_month.PPG == max_ppg]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="PPG:Q",
                    color=alt.value("#7defa1")
                )
                highlight_min = alt.Chart(ast_by_month[ast_by_month.PPG == min_ppg]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="PPG:Q",
                    color=alt.value("#ff2b2b")
                )

                # 4) Median line + annotation
                median_rule = alt.Chart(pd.DataFrame({"PPG":[median_ppg]})).mark_rule(
                    color="gray", strokeDash=[4,4]
                ).encode(y="PPG:Q")
                median_text = alt.Chart(pd.DataFrame({"PPG":[median_ppg]})).mark_text(
                    align="left", dx=3, dy=-5, color="gray"
                ).encode(
                    y="PPG:Q",
                )

                # 5) Layer everything and render
                chart = (
                    base
                    + highlight_max
                    + highlight_min
                    + median_rule
                    + median_text
                ).properties(
                    title="Monthly Points Per Game (PPG)",
                    width="container"
                )
                st.altair_chart(chart, use_container_width=True)  # type: ignore[arg-type]

                st.markdown(
                    f"""
                    <div style="
                        font-size: 0.875rem;
                        color: 888;
                        line-height: 1.4;
                        margin-top: 0.2rem;
                        padding: 0 0;
                    ">
                    The best month was <strong>{best_month}</strong> with <strong>{best_points}</strong>ast → (green bar).<br>
                    The worst month was <strong>{worst_month}</strong> with <strong>{worst_points}</strong>ast → (red bar).<br>
                    The league median is <strong>{median_ppg:.1f}</strong>pts → (dashed line).
                    </div>
                    """,
                    unsafe_allow_html=True
                )            

        with c_col3:
            if "reb" in df_team_metric.columns:
                # 1) Prepare the data
                df = df_team_metric.copy()
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                ast_by_month = (
                    df
                    .groupby(df["game_date"].dt.month)["reb"]
                    .mean()
                    .round(2)
                    .reset_index(name="RPG")
                )
                # preserve season order (Oct→Apr)
                order = (
                    df
                    .groupby(df["game_date"].dt.month)["game_date"]
                    .min()
                    .sort_values()
                    .index
                )
                # map numeric month → label
                ast_by_month["month"] = ast_by_month["game_date"].map(lambda m: months[m])
                x_domain = [months[m] for m in order]

        #         compute league median for reference
                median_RPG = df_gen["reb"].median()

                # identify max/min RPG values
                max_RPG = ast_by_month["RPG"].max()
                min_RPG = ast_by_month["RPG"].min()

                # now pull out exactly the best & worst months and their RPG
                best_idx    = ast_by_month["RPG"].idxmax()
                worst_idx   = ast_by_month["RPG"].idxmin()
                best_month  = ast_by_month.loc[best_idx,  "month"]
                best_points = ast_by_month.loc[best_idx,  "RPG"]
                worst_month = ast_by_month.loc[worst_idx, "month"]
                worst_points= ast_by_month.loc[worst_idx, "RPG"]

                # 2) Build the base bars (light blue)
                base = alt.Chart(ast_by_month).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain, title="Month"),
                    y=alt.Y("RPG:Q", title="RPG"),
                    color=alt.value("#83c9ff")
                )

                # 3) Highlight max & min
                highlight_max = alt.Chart(ast_by_month[ast_by_month.RPG == max_RPG]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="RPG:Q",
                    color=alt.value("#7defa1")
                )
                highlight_min = alt.Chart(ast_by_month[ast_by_month.RPG == min_RPG]).mark_bar().encode(
                    x=alt.X("month:N", sort=x_domain),
                    y="RPG:Q",
                    color=alt.value("#ff2b2b")
                )

                # 4) Median line + annotation
                median_rule = alt.Chart(pd.DataFrame({"RPG":[median_RPG]})).mark_rule(
                    color="gray", strokeDash=[4,4]
                ).encode(y="RPG:Q")
                median_text = alt.Chart(pd.DataFrame({"RPG":[median_RPG]})).mark_text(
                    align="left", dx=3, dy=-5, color="gray"
                ).encode(
                    y="RPG:Q",
                )

                # 5) Layer everything and render
                chart = (
                    base
                    + highlight_max
                    + highlight_min
                    + median_rule
                    + median_text
                ).properties(
                    title="Monthly Points Per Game (RPG)",
                    width="container"
                )
                st.altair_chart(chart, use_container_width=True)  # type: ignore[arg-type]

                st.markdown(
                    f"""
                    <div style="
                        font-size: 0.875rem;
                        color: 888;
                        line-height: 1.4;
                        margin-top: 0.2rem;
                        margin-bottom:2rem;
                        padding: 0 0;
                    ">
                    The best month was <strong>{best_month}</strong> with <strong>{best_points}</strong>ast → (green bar).<br>
                    The worst month was <strong>{worst_month}</strong> with <strong>{worst_points}</strong>ast → (red bar).<br>
                    The league median is <strong>{median_RPG:.1f}</strong>pts → (dashed line).
                    </div>
                    """,
                    unsafe_allow_html=True
                ) 