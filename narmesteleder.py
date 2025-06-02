# %% [markdown]
# ---
# title: syfo - narmesteleder
# title-block-banner: "#003768"
# title-block-banner-color: white
# theme: sandstone
# execute:
#     echo: false
#     warning: false
# format:
#     html:
#         toc: true
#         toc-title: Innhold
#         page-layout: full
#         embed-resources: true
# jupyter: python3
# ---

# %%
#| echo: false
import pandas as pd 
import pandas_gbq 
import sys
import re
import os
import random



from tools import (
    get_dict, 
    get_date_formats, 
    get_dwmy_df,
    dwm_bar_plot,
)

import plotly.express as px
from plotly import graph_objects as go
import plotly.io as pio
pio.renderers.default = ("plotly_mimetype+" + "notebook_connected+" + "iframe_connected")

# %%
project = 'teamsykefravr-prod-7e29'
d_sql = get_dict("isnarmesteleder.sql")


# %%
#| echo: false
#| output: false


df_l_s =pandas_gbq.read_gbq(d_sql['kobletnarmestelederaktivt'], project_id=project)

# %% [markdown]

# :::{.column-page}

### datasett-analyse
# %% [markdown]

# ::: {.panel-tabset}

#### Antall data over tid

#%%
df = get_date_formats(df_l_s, "aktiv_fom")
date_col='d'
df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
t_g = get_dwmy_df(df, date_col='d')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm
# %% [markdown]

####  status-typer
# %%

unike_statuser = df_l_s['status'].unique()

# 2. Tell antall av hver status
status_counts = df_l_s['status'].value_counts().reset_index()
status_counts.columns = ['status', 'count']

fig = px.bar(status_counts, x='status', y='count', color='status', color_discrete_sequence=px.colors.qualitative.Dark24 )

for trace in fig.data:
    trace.name = f"Status {trace.name}"  # Legger til "Varsler" foran typen

fig.update_layout(xaxis=dict(title='Status', showticklabels = False),
                  yaxis=dict(title="Antall"),
                  width=1000,
                  showlegend=True
                  )


fig.show()


# %% [markdown]

####  antall personer med og uten leder
# %%

# stor verdi 
df_uten = df_l_s[df_l_s['narmeste_leder_personident'].isna()]
antall_uten_leder = df_uten['fnr'].nunique()

totalt_antall_personer = df_l_s['fnr'].nunique()
antall_med_leder = totalt_antall_personer - antall_uten_leder

data = {
    "Lederstatus": ["Har leder", "Ingen leder"],
    "Antall personer": [antall_med_leder, antall_uten_leder]
}
df_plot = pd.DataFrame(data)

fig = px.bar(
    df_plot,
    x="Lederstatus",
    y="Antall personer",
    color="Lederstatus",
    text="Antall personer",
    title="Antall personer med og uten nærmeste leder",
    color_discrete_map={
        "Har leder": "seagreen",
        "Ingen leder": "crimson"
    }
)

fig.update_layout(
    xaxis_title="Lederstatus",
    yaxis_title="Antall personer",
    showlegend=False
)

fig.show()

# %% [markdown]

#### Fordeling av aktive ledere per person (Status "NY_LEDER")
# %%

df = df_l_s.sort_values(by=["fnr", "narmeste_leder_personident", "aktiv_fom"]).copy()


aktive_relasjoner = []


for (fnr, leder), gruppe in df.groupby(["fnr", "narmeste_leder_personident"]):
    # Finn den siste aktiv_tom hvis den finnes
    siste_aktiv_tom = gruppe["aktiv_tom"].dropna().max()
    
    if pd.notna(siste_aktiv_tom):
        
        filtrert = gruppe[gruppe["aktiv_fom"] > siste_aktiv_tom]
    else:
        filtrert = gruppe
    
    ny_leder_rader = filtrert[filtrert["status"] == "NY_LEDER"]
    if not ny_leder_rader.empty:
        siste_ny_leder = ny_leder_rader.sort_values(by="aktiv_fom").iloc[[-1]]  # behold som DataFrame
        aktive_relasjoner.append(siste_ny_leder)

aktive_df = pd.concat(aktive_relasjoner, ignore_index=True)

antall_aktive = aktive_df["fnr"].nunique()

flere_ledere = aktive_df.groupby("fnr")["narmeste_leder_personident"].nunique()
personer_med_flere_ledere = flere_ledere[flere_ledere > 1]


antall_med_1_leder = (flere_ledere == 1).sum()
antall_med_flere_ledere = (flere_ledere > 1).sum()

data2 = pd.DataFrame({
    "Leder-type": ["1 aktiv leder", "Flere aktive ledere"],
    "Antall": [antall_med_1_leder, antall_med_flere_ledere]
})

fig2 = px.bar(data2, x="Leder-type", y="Antall", text="Antall", color="Leder-type", color_discrete_sequence=px.colors.qualitative.Set1)
fig2.show()

# %% [markdown]

#### Fordelling leder

# %%

# Alle unike personer i original datasett
alle_personer = df_l_s["fnr"].nunique()
personer_med_aktiv_leder = aktive_df["fnr"].nunique()
personer_uten_aktiv_leder = alle_personer - personer_med_aktiv_leder

data1 = pd.DataFrame({
    "Status": ["Med aktiv leder", "Uten aktiv leder"],
    "Antall": [personer_med_aktiv_leder, personer_uten_aktiv_leder]
})

fig1 = px.bar(data1, x="Status", y="Antall", title="Antall personer med / uten aktiv leder",
              text="Antall", color="Status", color_discrete_sequence=px.colors.qualitative.Set2)
fig1.show()
# Beregn antall ledere per person
flere_ledere = aktive_df.groupby("fnr")["narmeste_leder_personident"].nunique()
antall_med_1 = (flere_ledere == 1).sum()
antall_med_flere = (flere_ledere > 1).sum()


# %% [markdown]

#### Beregn antall ledere per person med status "NY_LEDER" og "None"

# %%
def hent_aktive_ledere(df_l_s):
    # Trinn 1: Sorter data kronologisk per person og leder
    df = df_l_s.sort_values(by=["fnr", "narmeste_leder_personident", "aktiv_fom"]).copy()

    aktive_relasjoner_none = []

    # Trinn 2: Gå gjennom hver (person, leder)-gruppe
    for (fnr, leder), gruppe in df.groupby(["fnr", "narmeste_leder_personident"]):
        # Finn siste avslutningsdato hvis det finnes
        siste_aktiv_tom = gruppe["aktiv_tom"].dropna().max()

        if pd.notna(siste_aktiv_tom):
            # Behold kun rader der aktiv_fom er etter avslutning
            filtrert = gruppe[gruppe["aktiv_fom"] > siste_aktiv_tom]
        else:
            # Ingen avslutning – behold alt
            filtrert = gruppe

        # Ta med både 'NY_LEDER' og manglende status (None)
        ny_leder_rader = filtrert[filtrert["status"].isin(["NY_LEDER", None])]

        if not ny_leder_rader.empty:
            # Behold kun den siste
            siste_ny_leder = ny_leder_rader.sort_values(by="aktiv_fom").iloc[[-1]].copy()

            # Legg til merknad om status mangler
            if pd.isna(siste_ny_leder["status"].values[0]):
                siste_ny_leder["merknad"] = "status mangler"
            else:
                siste_ny_leder["merknad"] = "OK"

            aktive_relasjoner_none.append(siste_ny_leder)

    # Slå sammen aktive relasjoner til én DataFrame
    if aktive_relasjoner_none:
        aktive_df_none = pd.concat(aktive_relasjoner_none, ignore_index=True)
    else:
        aktive_df_none = pd.DataFrame()

    return aktive_df_none
# Bruk funksjonen på datasettet ditt
aktive_df_none = hent_aktive_ledere(df_l_s)

flere_ledere_none = aktive_df_none.groupby("fnr")["narmeste_leder_personident"].nunique()
antall_med_1_none = (flere_ledere_none == 1).sum()
antall_med_flere_none = (flere_ledere_none > 1).sum()

data2 = pd.DataFrame({
    "Leder-type": ["1 aktiv leder", "Flere aktive ledere"],
    "Antall": [antall_med_1_none, antall_med_flere_none]
})

fig2 = px.bar(data2, x="Leder-type", y="Antall", 
              text="Antall", color="Leder-type", color_discrete_sequence=px.colors.qualitative.Set1)
fig2.show()


# %% [markdown]

#### Fordeling av aktive ledere per person (Status: "NY_LEDER" og "None")

# %%

# Alle unike personer i original datasett
alle_personer = df_l_s["fnr"].nunique()
personer_med_aktiv_leder_none = aktive_df_none["fnr"].nunique()
personer_uten_aktiv_leder_none = alle_personer - personer_med_aktiv_leder_none

data1 = pd.DataFrame({
    "Status": ["Med aktiv leder", "Uten aktiv leder"],
    "Antall": [personer_med_aktiv_leder_none, personer_uten_aktiv_leder_none]
})

fig1 = px.bar(data1, x="Status", y="Antall", title="Antall personer med / uten aktiv leder",
              text="Antall", color="Status", color_discrete_sequence=px.colors.qualitative.Set2)
fig1.show()

# %% [markdown]
# :::


# %% [markdown]
# :::


