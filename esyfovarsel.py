
# %% [markdown]
# ---
# title: syfo - esyfovarsel
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
import numpy as np
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
    lag_sankey_fra_hendelser1,
    lag_hendelsesflyt_og_beregn_tid1,
    utc_to_local
)

import datetime as dt
import plotly.express as px
from plotly import graph_objects as go
import plotly.io as pio
pio.renderers.default = ("plotly_mimetype+" + "notebook_connected+" + "iframe_connected")


# %%
project = 'teamsykefravr-prod-7e29'
d_sql = get_dict("esyfovarsel.sql")

# %%
#| echo: false
#| output: false
# alle utsendte varsler
df = pandas_gbq.read_gbq(d_sql['esyfovarsel_alt'], project_id=project)
df['år'] = df.utsendt_tidspunkt.astype('datetime64[ns]').dt.strftime('%Y')
df = get_date_formats(df, "utsendt_tidspunkt")

# alle feilede varsler
df_f = pandas_gbq.read_gbq(d_sql['esyfovarsel_feilet_utsending'], project_id=project)
df_f = get_date_formats(df_f, "utsendt_forsok_tidspunkt")

# alle kalenderavtale
df_k = pandas_gbq.read_gbq(d_sql['esyfovarsel_kalenderavtale'],project_id=project)
df_k = get_date_formats(df_k, "opprettet")

# alle dialogmote isyfo
df_d = pandas_gbq.read_gbq(d_sql['isyfo_mote_status_endret'],project_id=project)
df_d = get_date_formats(df_d, "created_at")

# alle friskmelding til arbeidsformidling isyfo
df_fta = pandas_gbq.read_gbq(d_sql['isyfo_friskmelding_til_arbeidsformidling'],project_id=project)
df_fta = utc_to_local(df_fta)
df_fta = get_date_formats(df_fta, "created_at")

# alle mikrofrontend-synlighet
df_s = pandas_gbq.read_gbq(d_sql['esyfovarsel_mikrofrontend_synlighet'],project_id=project)
df_s = get_date_formats(df_s, "opprettet")
#%%
print(f'Sist oppdatert: {dt.datetime.now().strftime("%Y-%m-%d %H:%M")}')


# %% [markdown]

# :::{.column-page}

#
# esyfovarsel analysen består av
#
# - videresendte varsler for forskjellige typer tema (topic) til relevante kanaler: 
#
#       - DineSykmeldte
#
#       - Brukernotifikasjoner 
#
#       - Arbeidsgivernotifikasjoner
#
# - Kalenderavtale for arbeidsgivere som viser kommende avtale (typisk dialogmøte). 

### Utsendte varsler
# %% [markdown]
# ::: {.panel-tabset}
#
#  Denne seksjonen viser antallet utsendte varsler over tid, fordelt på ulike kanaler.  
#  Den viser også hvor ofte ulike typer varsler forekommer (frekvens).

#### Antall utsendte varsler

# %%
t_g = get_dwmy_df(df, date_col='utsendt_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm


# %% [markdown]

####  Antall utsendte varsler per ukedag
# %%
# Gruppere etter ukedag og telle antall forekomster
weekday_counts = df.groupby('dw').size().reset_index(name='antall')

# Sortere ukedagene i riktig rekkefølge
weekday_counts['dw'] = pd.Categorical(weekday_counts['dw'], categories=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ordered=True)
weekday_counts = weekday_counts.sort_values('dw')

# Lag stolpediagram for ukedagsanalyse
fig_weekday = px.bar(weekday_counts, x='dw', y='antall', color='dw', color_discrete_sequence=px.colors.qualitative.Dark24)
fig_weekday.update_layout(xaxis=dict(title="Uke"),
                  yaxis=dict(title="Antall"),
                  width=1000,
                  showlegend=False
                  )


# Vis diagrammet
fig_weekday.show()

# %% [markdown]

####  Antall Frekvensanalyse av Varsler per Type
# %% 

gr3 = df['type'].value_counts().reset_index()
gr3.columns = ['type', 'antall']

fig = px.bar(gr3, x='type', y='antall',   color='type', color_discrete_sequence=px.colors.qualitative.Dark24 )
for trace in fig.data:
    trace.name = f"Varsler {trace.name}"  # Legger til "Varsler" foran typen

fig.update_layout(xaxis=dict(title="Varsler type", showticklabels = False),
                  yaxis=dict(title="Antall"),
                  width=1000,
                  showlegend=True
                  )


fig.show()

# %% [markdown]
#### Antall utsendte varsler med kanal BRUKERNOTIFIKASJON

# %%

t_g = get_dwmy_df(df[df.kanal == 'BRUKERNOTIFIKASJON'], date_col='utsendt_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm

# %% [markdown]
#### Antall utsendte varsler med kanal DITT_SYKEFRAVAER

# %%

t_g = get_dwmy_df(df[df.kanal == "DITT_SYKEFRAVAER"], date_col='utsendt_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm


# %% [markdown]
#### Antall utsendte varsler med kanal DINE_SYKMELDTE 

# %%

t_g = get_dwmy_df(df[df.kanal == "DINE_SYKMELDTE"], date_col='utsendt_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm


# %% [markdown]
#### Antall utsendte varsler med kanal ARBEIDSGIVERNOTIFIKASJON

# %%

t_g = get_dwmy_df(df[df.kanal == 'ARBEIDSGIVERNOTIFIKASJON'], date_col='utsendt_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm


# %% [markdown]
#### Antall utsendte varsler med kanal BREV

# %%

t_g = get_dwmy_df(df[df.kanal == "BREV"], date_col='utsendt_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm



# %% [markdown]
# :::


# %% [markdown]

### Utsendte varsler feilet

# ::: {.panel-tabset}

#### Antall feilede totalt

# %%

t_g = get_dwmy_df(df_f, date_col='utsendt_forsok_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm

# %% [markdown]
#### Antall feilede etter 14.03.25

# %%

t_g = get_dwmy_df(df_f[df_f.utsendt_forsok_tidspunkt > "2025-03-15"], date_col='utsendt_forsok_tidspunkt', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm

# %% [markdown]
#### Antall feilede per type totalt, eksl. vellykket resending
# %%

gr = df_f[df_f.is_resendt==False].groupby('d').hendelsetype_navn.value_counts(normalize=False).reset_index(name="nc")

fig = px.bar(gr, x="d", y="nc", color="hendelsetype_navn")

fig.update_layout(xaxis=dict(title="Dag feilet"),
                  yaxis=dict(title="Antall"),
                  width=1000)

# %% [markdown]
#### Antall feilede per type etter 14.03.25, eksl. vellykket resending
# %%

gr = df_f[(df_f.is_resendt==False) & (df_f.utsendt_forsok_tidspunkt > "2025-03-15")].groupby('d').hendelsetype_navn.value_counts(normalize=False).reset_index(name="nc")

fig = px.bar(gr, x="d", y="nc", color="hendelsetype_navn")

fig.update_layout(xaxis=dict(title="Dag feilet"),
                  yaxis=dict(title="Antall"),
                  width=1000)

# %% [markdown]
#### Antall feilede per kanal totalt, eksl. vellykket resending
# %%

gr = df_f[(df_f.is_resendt==False)].groupby('d').kanal.value_counts(normalize=False).reset_index(name="nc")

fig = px.bar(gr, x="d", y="nc", color="kanal")

fig.update_layout(xaxis=dict(title="Dag feilet"),
                  yaxis=dict(title="Antall"))

# %% [markdown]
#### Antall feilede per kanal etter 14.03.25

# %%

gr = df_f[(df_f.is_resendt==False) & (df_f.utsendt_forsok_tidspunkt > "2025-03-15")].groupby('d').kanal.value_counts(normalize=False).reset_index(name="nc")

fig = px.bar(gr, x="d", y="nc", color="kanal")

fig.update_layout(xaxis=dict(title="Dag feilet"),
                  yaxis=dict(title="Antall"))


# %% [markdown]
####  Antal varsler og differanse mellom INNKALT og SM_DIALOGMOTE_INNKALT etter 2023-08-21 

#%%

t_g_i_nye = get_dwmy_df(df_d[(df_d['status'] == 'INNKALT') & (df_d['created_at'] > '2023-08-21')], date_col='created_at', week_col='yw', month_col='ym')
t_g_e_k = get_dwmy_df(df[(df['type'] == 'SM_DIALOGMOTE_INNKALT') & (df['kanal'] == 'DITT_SYKEFRAVAER')], date_col='utsendt_tidspunkt', week_col='yw', month_col='ym')

g_week_e = next(df for df, label in t_g_e_k if label == "Uke")
g_week_i_nye = next(df for df, label in t_g_i_nye if label == "Uke")

 

df_e = g_week_e[:-1].rename(columns={'n_count': 'e_syfo'})
df_i = g_week_i_nye[:-1].rename(columns={'n_count': 'i_syfo'})

df_diff = df_i.merge(df_e, on='Tid', how='outer').fillna(0)
df_diff['diff'] = df_diff['i_syfo'] - df_diff['e_syfo']


df_diff['diff_plot'] = df_diff['diff'].apply(lambda x: x if x > 0 else np.nan)


fig = px.bar(
    df_diff,
    x='Tid',
    y='diff_plot',
    #title='Positiv differanse per uke (i_syfo - e_syfo)',
    labels={'Tid': 'Uke', 'diff_plot': 'Differanse'}
)

fig.update_layout(
    xaxis=dict(title='Uke', type='category'),
    yaxis=dict(title='Differanse'),
    template='plotly_white',
    width=900,
    height=500
)

fig.show()

# %% [markdown]
# :::

# %% [markdown]

### Kalenderavtale 

# ::: {.panel-tabset}

#### Antall opprettet avtaler

# %%
t_g = get_dwmy_df(df_k, date_col='opprettet', week_col='yw', month_col='ym')

fig_dwm = dwm_bar_plot(t_g)

fig_dwm



# %% [markdown]
#### Kalendertilstand typer
# %%


gr = df_k.groupby('d')['kalenderavtaletilstand'].value_counts(normalize=False).reset_index(name='nc')

fig = px.bar(gr, x="d", y="nc", color="kalenderavtaletilstand")

fig.update_layout(xaxis=dict(title="Kalendertilstand"),
                  yaxis=dict(title="Antall"),
                  width=1000)



# %% [markdown]
#### Flyt mellom Kalendertilstand (kalenderid)

#%%
df_final_1 = lag_hendelsesflyt_og_beregn_tid1(df_k)
lag_sankey_fra_hendelser1(df_final_1)

# %% [markdown]
#### Antall per Siste Kalendertilstand basert på kalenderid
#%%

siste_tilstand_count = df_final_1['siste_tilstand'].value_counts().reset_index()
siste_tilstand_count.columns = ['siste_tilstand', 'count']


fig = px.bar(siste_tilstand_count, x='siste_tilstand', y='count',
             labels={'siste_tilstand': 'Kalendertilstand', 'count': 'Antall Kalenderid'},
             color='siste_tilstand', color_discrete_sequence=px.colors.qualitative.Set2)

# Vis diagrammet
fig.show()

# %% [markdown]
# :::
# %% [markdown]
# :::

