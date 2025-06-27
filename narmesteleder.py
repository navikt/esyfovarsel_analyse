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
    utc_to_local
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

df_nl_r =pandas_gbq.read_gbq(d_sql['narmesteleder_restricted'], project_id=project)

df_l_s =pandas_gbq.read_gbq(d_sql['kobletnarmestelederaktivt'], project_id=project)
# %% [markdown]

# :::{.column-page}
#
### Narmeste-leder-relasjoner analyse
# Datasettet som brukes her, gjelder narmeste-leder-relasjoner og er begrenset til år 2025.
# %% [markdown]

# ::: {.panel-tabset}

#### Antall data per uke i 2025

#%%
df = utc_to_local(df_nl_r)
df = get_date_formats(df, "created_at")
df['yw_str'] = df['yw'].astype(str)
df['uke_value'] = df['yw_str'].str.split('-').str[1].astype(int)
df['uke_label'] = 'Uke ' + df['uke_value'].astype(str)

counts = df.groupby(['uke_label', 'status']).size().reset_index(name='antall')
counts['uke_sort'] = counts['uke_label'].str.extract(r'(\d+)').astype(int)
counts = counts.sort_values('uke_sort')

fig = px.bar(
    counts,
    x='uke_label',
    y='antall',
    color='status',
    barmode='stack',
    category_orders={'uke_label': counts['uke_label'].unique()},
    labels={'uke_label': 'Uke', 'antall': 'Antall', 'status': 'Status'}
)
fig.update_layout(
    xaxis_title='Uke',
    yaxis_title='Antall',
    legend_title_text='Status',
    xaxis_tickangle=-45,
    bargap=0.3
)
fig.show()



#  %% [markdown]
# :::

# %% [markdown]

### Koblet-Datasett analyse
# Datasettet som brukes i analysen, er en kobling av aktive sykmeldte i SYFO og nærmeste-leder-relasjon i SYFO.
#

# ::: {.panel-tabset}

#### Antall data per År

#%%

df = utc_to_local(df_l_s)
df = get_date_formats(df, "leder_created_at")
df['d_str'] = df['d'].astype(str)
df['aar_value'] = df['d_str'].str.split('-').str[0]

counts = df.groupby(['aar_value', 'status']).size().reset_index(name='antall')
counts['aar_sort'] = counts['aar_value'].str.extract(r'(\d+)').astype(int)
counts = counts.sort_values('aar_sort')

fig = px.bar(
    counts,
    x='aar_value',
    y='antall',
    color='status',
    barmode='stack',
    category_orders={'aar_value': counts['aar_value'].unique()},
    labels={'aar_value': 'Uke', 'antall': 'Antall', 'status': 'Status'}
)
fig.update_layout(
    xaxis_title='Uke',
    yaxis_title='Antall',
    legend_title_text='Status',
    xaxis_tickangle=-45,
    bargap=0.3
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
    text=("Denne viser lederstatus for sykmeldte i Syfo. Med leder mener vi de som har en leder registrert i narmeste_leder_relajson, og uten leder er de som ikke har noen data i narmeste_leder_relajson"),
    xref="paper", yref="paper",
    x=0, y=-0.3,
    showarrow=False,
    font=dict(size=12, color="grey"),
    align="left"
)
fig.show()
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
    "Datagrunnlag: status med NY_LEDER og None bare.<br>"
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

#### Fordeling av ledere per person inkludert kun avsluttede relasjoner
# %%

personer_med_aktiv_leder_none = aktive_df_none["fnr"].nunique()
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
        "Datagrunnlag: status med NY_LEDER og None bare.<br>"
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

#### Fordeling av ledere etter antall virksomheter de leder
# %%
leder_per_virksomhet_r2 =aktive_df_none.groupby('narmeste_leder_personident')['virksomhetsnummer'].nunique().reset_index()

vc = leder_per_virksomhet_r2['virksomhetsnummer'].value_counts().reset_index()
vc.columns = ['antall_virksomheter', 'antall_ledere']

# sortere etter antall virksomheter for bedre x-akse
vc = vc.sort_values('antall_virksomheter')

fig = px.bar(vc,
             x='antall_virksomheter',
             y='antall_ledere',
             text='antall_ledere',
             labels={'antall_virksomheter': 'Antall virksomheter per leder',
                     'antall_ledere': 'Antall ledere'},
             title='')

fig.update_traces(textposition='outside')
fig.update_layout(xaxis=dict(dtick=1))  # alle x-ticks
fig.add_annotation(
    text=(
        "Datagrunnlag: status med NY_LEDER og None bare.<br>"
    ),
    xref="paper", yref="paper",
    x=0, y=-0.5,
    showarrow=False,
    font=dict(size=12, color="grey"),
    align="left"
)
fig.show()

# %% [markdown]

#### Fordeling av virksomheter etter antall aktive ledere (gruppert)
#%%
leder_per_virksomhet_r1 =aktive_df_none.groupby('virksomhetsnummer')['narmeste_leder_personident'].nunique().reset_index()
leder_per_virksomhet_r1.columns = ['virksomhetsnummer', 'antall_ledere']
grense = 6

def etikett_ny(x):
    if x <= grense:
        return str(x)
    else:
        return f"over {grense}"

# Bruk funksjonen for å lage etikett-kolonne med ny gruppering
leder_per_virksomhet_r1['leder_etikett'] = leder_per_virksomhet_r1['antall_ledere'].apply(etikett_ny)

# Tell antall virksomheter per etikett
fordeling1 = leder_per_virksomhet_r1.groupby('leder_etikett').size().reset_index(name='Antall virksomheter')

# Lag sorteringsrekkefølge (sørg for at 'over 6' kommer sist)
sorteringsrekkefølge1 = {str(i): i for i in range(1, grense+1)}
sorteringsrekkefølge1[f"over {grense}"] = grense + 1

fordeling1['sort_key'] = fordeling1['leder_etikett'].map(sorteringsrekkefølge1)

# Sorter etter nøkkel og fjern kolonne
fordeling1 = fordeling1.sort_values('sort_key').drop(columns='sort_key')




fig = px.bar(
    fordeling1,
    x='leder_etikett',
    y='Antall virksomheter',
    text='Antall virksomheter'
)

fig.update_layout(
    #title="Antall virksomheter etter antall aktive ledere (gruppert)",
    xaxis_title=" Antall ledere per virksomhet(gruppert)",
    yaxis_title="Antall virksomheter",
    margin=dict(b=120)
)

fig.show()

# %% [markdown]

#### Antall ansatte per leder
#%%

gr1 = aktive_df_none.groupby(['narmeste_leder_personident'])['fnr'].nunique().reset_index(name='antall_ansatte')

grense = 8

def etikett_ny(x):
    if x <= grense:
        return str(x)
    else:
        return f"over {grense}"


gr1['etikett'] = gr1['antall_ansatte'].apply(etikett_ny)

fordeling1 = gr1.groupby('etikett').size().reset_index(name='Antall_ansatte')

sorteringsrekkefølge1 = {str(i): i for i in range(1, grense+1)}
sorteringsrekkefølge1[f"over {grense}"] = grense + 1

fordeling1['sort_key'] = fordeling1['etikett'].map(sorteringsrekkefølge1)


fordeling1 = fordeling1.sort_values('sort_key').drop(columns='sort_key')




fig = px.bar(
    fordeling1,
    x='etikett',
    y='Antall_ansatte',
    text='Antall_ansatte'
)

fig.update_layout(
    #title="Antall ansatte per leder(gruppert))",
    xaxis_title="Antall ansatte",
    yaxis_title="Antall ledere",
    margin=dict(b=120)
)

fig.show()

#  %% [markdown]
# :::


# %% [markdown]
# :::


