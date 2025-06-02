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
#
# Datasettet som brukes i analysen, er en kobling av aktive sykmeldte i SYFO og nærmeste-leder-relasjon i SYFO.
### Datasett-analyse
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

####  Antall personer med og uten leder
# %%

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
    #title="Antall personer med og uten nærmeste leder",
    color_discrete_map={
        "Har leder": "seagreen",
        "Ingen leder": "crimson"
    }
)

fig.update_layout(
    xaxis_title="Lederstatus",
    yaxis_title="Antall personer",
    showlegend=False,
    margin=dict(b=100)  # Gir plass nederst til annotasjonen
)

# Legg til footnote
fig.add_annotation(
    text="Denne viser lederstatus for sykmeldte i Syfo. Med leder mener vi de som har en leder registrert i narmeste_leder_relajson, og uten leder er de som ikke har noen data i narmeste_leder_relajson",
    xref="paper", yref="paper",
    x=0, y=-0.3,
    showarrow=False,
    font=dict(size=12, color="grey"),
    align="left"
)
fig.show()
#  %% [markdown]
# :::


# %% [markdown]

### Aktiv ledere ANALYSE MED Status "NY_LEDER"

# ::: {.panel-tabset}

#
# Denne seksjon viser person-leder analyse med status "NY_LEDER". Den vises hvem som har en eller fler aktiv leder og hvem som har avslutted leder relasjon og har ingen nye relasjon.

####  Antall personer med en eller flere ledere
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
fig2.update_layout(
    margin=dict(b=200)  # Mer plass under grafen
)
fig2.add_annotation(
    text = (
    "Denne figuren viser aktiv lederstatus for sykmeldte i Syfo.<br>"
    "Med 1 aktiv leder mener vi de som har én aktiv leder registrert i narmeste_leder_relajson.<br>"
    "Med flere aktive ledere mener vi de som har mer enn én leder registrert som aktiv i narmeste_leder_relajson."
),
    xref="paper", yref="paper",
    x=0, y=-0.5,
    showarrow=False,
    font=dict(size=12, color="grey"),
    align="left"
)

fig2.show()

# %% [markdown]

#### Antall personer med / uten aktiv leder

# %%

# Alle unike personer i original datasett
alle_personer = df_l_s["fnr"].nunique()
personer_med_aktiv_leder = aktive_df["fnr"].nunique()
personer_uten_aktiv_leder = alle_personer - personer_med_aktiv_leder

data1 = pd.DataFrame({
    "Status": ["Med aktiv leder", "Uten aktiv leder"],
    "Antall": [personer_med_aktiv_leder, personer_uten_aktiv_leder]
})

fig1 = px.bar(data1, x="Status", y="Antall", #title="Antall personer med / uten aktiv leder",
              text="Antall", color="Status", color_discrete_sequence=px.colors.qualitative.Set2)

fig1.show()

# %% [markdown]

#### Fordeling av ledere per person inkludert kun avsluttede relasjoner


# %%

#title="Fordeling av ledere inkludert kun avsluttede relasjoner",
# Alle personer som har minst én relasjon med aktiv_tom satt (altså avsluttet)
personer_med_avsluttede1 = df_l_s[df_l_s["aktiv_tom"].notna()]["fnr"].unique()

# Alle personer med aktive relasjoner (funnet fra hent_aktive_ledere)
aktive_fnr = set(aktive_df["fnr"])

# De som kun har avsluttede relasjoner, altså ikke aktive nå
kun_avsluttede_fnr1 = set(personer_med_avsluttede1) - aktive_fnr

# Tell hvor mange slike personer det er
antall_kun_avsluttede1 = len(kun_avsluttede_fnr1)


data_extended1 = pd.DataFrame({
    "Fordeling": [
        "Med aktiv leder",
        "Tidligere leder - kun avsluttet relasjon",
        "Uten leder"
    ],
    "Antall": [
        personer_med_aktiv_leder,
        antall_kun_avsluttede1,
        antall_uten_leder
    ]
})

fig = px.bar(
    data_extended1, x="Fordeling", y="Antall",
    
    text="Antall", color="Fordeling",
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig.update_layout(
    margin=dict(b=200)  # Mer plass under grafen
)
fig.add_annotation(
    text=(
        "Denne viser lederstatus for sykmeldte i Syfo.<br>"
        "Med aktiv leder mener vi de som har én eller flere aktive ledere registrert i narmeste_leder_relajson.<br>"
        "Tidligere leder — kun avsluttet relasjon — er de som har hatt leder, men ikke har noen aktive nå.<br>"
        "Uten leder er de som ikke har noen data i narmeste_leder_relajson."
    ),
    xref="paper", yref="paper",
    x=0, y=-0.5,
    showarrow=False,
    font=dict(size=12, color="grey"),
    align="left"
)


fig.show()


# %% [markdown]
# :::

# %% [markdown]

### Aktiv ledere ANALYSE MED Status "NY_LEDER" og "None"

# ::: {.panel-tabset}

#
# Denne seksjon viser person-leder analyse med status "NY_LEDER" og "None". Her vi ser på er det noen som har leder men har status "None". Den vises også hvem som har en eller flere aktiv leder og hvem som har avslutted leder relasjon og har ingen nye relasjon.
#

# %% [markdown]

#### Antall personer med en eller flere ledere

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

fig2.update_layout(
    margin=dict(b=200)  # Mer plass under grafen
)
fig2.add_annotation(
    text = (
    "Denne figuren viser aktiv lederstatus for sykmeldte i Syfo.<br>"
    "Med 1 aktiv leder mener vi de som har én aktiv leder registrert i narmeste_leder_relajson.<br>"
    "Med flere aktive ledere mener vi de som har mer enn én leder registrert som aktiv i narmeste_leder_relajson."
),
    xref="paper", yref="paper",
    x=0, y=-0.5,
    showarrow=False,
    font=dict(size=12, color="grey"),
    align="left"
)

fig2.show()


# %% [markdown]

#### Antall personer med / uten aktiv leder

# %%

# Alle unike personer i original datasett
alle_personer = df_l_s["fnr"].nunique()
personer_med_aktiv_leder_none = aktive_df_none["fnr"].nunique()
personer_uten_aktiv_leder_none = alle_personer - personer_med_aktiv_leder_none

data1 = pd.DataFrame({
    "Status": ["Med aktiv leder", "Uten aktiv leder"],
    "Antall": [personer_med_aktiv_leder_none, personer_uten_aktiv_leder_none]
})

fig1 = px.bar(data1, x="Status", y="Antall", #title="Antall personer med / uten aktiv leder",
              text="Antall", color="Status", color_discrete_sequence=px.colors.qualitative.Set2)
fig1.show()


# %% [markdown]

#### Fordeling av ledere per person inkludert kun avsluttede relasjoner
# %%

# Alle personer som har minst én relasjon med aktiv_tom satt (altså avsluttet)
personer_med_avsluttede = df_l_s[df_l_s["aktiv_tom"].notna()]["fnr"].unique()

# Alle personer med aktive relasjoner (funnet fra hent_aktive_ledere)
aktive_fnr = set(aktive_df_none["fnr"])

# De som kun har avsluttede relasjoner, altså ikke aktive nå
kun_avsluttede_fnr = set(personer_med_avsluttede) - aktive_fnr

# Tell hvor mange slike personer det er
antall_kun_avsluttede = len(kun_avsluttede_fnr)

data_extended = pd.DataFrame({
    "Fordeling": [
        "Med aktiv leder",
        "Tidligere leder - kun avsluttet relasjon",
        "Uten leder"
    ],
    "Antall": [
        personer_med_aktiv_leder_none,
        antall_kun_avsluttede,
        antall_uten_leder
    ]
})

fig = px.bar(
    data_extended, x="Fordeling", y="Antall",
    
    text="Antall", color="Fordeling",
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig.update_layout(
    margin=dict(b=200)  # Mer plass under grafen
)
fig.add_annotation(
    text=(
        "Denne viser lederstatus for sykmeldte i Syfo.<br>"
        "Med aktiv leder mener vi de som har én eller flere aktive ledere registrert i narmeste_leder_relajson.<br>"
        "Tidligere leder — kun avsluttet relasjon — er de som har hatt leder, men ikke har noen aktive nå.<br>"
        "Uten leder er de som ikke har noen data i narmeste_leder_relajson."
    ),
    xref="paper", yref="paper",
    x=0, y=-0.5,
    showarrow=False,
    font=dict(size=12, color="grey"),
    align="left"
)

fig.show()





#  %% [markdown]
# :::


# %% [markdown]
# :::


