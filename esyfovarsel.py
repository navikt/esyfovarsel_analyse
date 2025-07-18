
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
df_m = pandas_gbq.read_gbq(d_sql['esyfovarsel_mikrofrontend_synlighet'],project_id=project)

# alle dialogmote med Nytt Tid Sted isyfo
df_n = pandas_gbq.read_gbq(d_sql['isyfo_dialog_status_endret_sykmeldte'],project_id=project)

# alle dialogmote med alle status og personident isyfo
df_n_f = pandas_gbq.read_gbq(d_sql['isyfo_alt_dialog_status_endret_sykmeldte'], project_id=project)

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

### Mikrofrontend synlighet

# ::: {.panel-tabset}

#### Antall opprettet synlighet i 2025

# %%
#| echo: false
#| warning: false

df_2025 = df_m[df_m['opprettet'] >= '2025-01-01'].copy()
df_2025 = get_date_formats(df_2025, "opprettet")

df_2025['uke_value'] = df_2025['yw'].str.split('-').str[1].astype(int)
#df_2025['uke_label'] = 'Uke ' + df_2025['uke_value'].astype(str)

counts = df_2025.groupby(['uke_value', 'tjeneste']).size().reset_index(name='antall')
#counts['uke_sort'] = counts['uke_value'].str.extract(r'(\d+)').astype(int)
#counts = counts.sort_values('uke_sort')

fig = px.bar(
    counts,
    x='uke_value',
    y='antall',
    color='tjeneste',
    barmode='stack',
    #category_orders={'uke_label': counts['uke_label'].unique()},
    labels={'uke_value': 'Uke', 'antall': 'Antall', 'tjeneste': 'Tjeneste'}
)
fig.update_layout(
    xaxis_title='Uke',
    yaxis_title='Antall',
    legend_title_text='Tjeneste',
    xaxis_tickangle=-45,
    bargap=0.3
)
fig.show()

# %% [markdown]
#### Fordelling av tjenester etter synlighetsperiode i uker (2025)
#%% 
#| echo: false
#| warning: false
df_2025['synlig_uker'] =( (pd.to_datetime(df_2025['synlig_tom']) - pd.to_datetime(df_2025['opprettet'])).dt.days // 7)

def grupper_varighet(uker):
    if uker <= 6:
        return "0-6 uker"
    elif uker <= 15:
        return "7-15 uker"
    elif uker <= 30:
        return "16-30 uker"
    else:
        return "Over 30 uker"

df_2025['varighetsgruppe'] = df_2025['synlig_uker'].apply(grupper_varighet)


gr = df_2025.groupby(['tjeneste', 'varighetsgruppe']).size().reset_index(name='antall')


# --- 6. Sett ønsket rekkefølge på gruppene
gruppe_rekkefølge = ["0-6 uker", "7-15 uker", "16-30 uker", "Over 30 uker"]
gr['varighetsgruppe'] = pd.Categorical(gr['varighetsgruppe'], categories=gruppe_rekkefølge, ordered=True)

# --- 7. Plot med Plotly
fig = px.bar(gr,
             x="varighetsgruppe",
             y="antall",
             color="tjeneste",
             title="Tjenester fordelt på varighetsgrupper",
             labels={
                 "varighetsgruppe": "Varighet",
                 "antall": "Antall",
                 "tjeneste": "Tjeneste"
             },
             category_orders={"varighetsgruppe": gruppe_rekkefølge})

fig.show()



# %% [markdown]
#### Fordelling av Dialogmøte etter synlighetsperiode i uker (2025)
#%% 

gr_dialogmote = gr[gr['tjeneste'] == 'DIALOGMOTE']

# --- 7. Plot med Plotly
fig = px.bar(gr_dialogmote,
             x="varighetsgruppe",
             y="antall",
             #color="tjeneste",
             title="Tjenester fordelt på varighetsgrupper",
             labels={
                 "varighetsgruppe": "Varighet",
                 "antall": "Antall",
                 #"tjeneste": "Tjeneste"
             },
             category_orders={"varighetsgruppe": gruppe_rekkefølge})

fig.show()

# %% [markdown]
#### Grunn for Dialogmøte synlighet i over 30 uker (2025)
### har den status NYTT_TID_STED?
#%% 
df_m_d = df_2025[(df_2025['tjeneste'] == 'DIALOGMOTE')].copy()


df_n['created_at_mse'] = pd.to_datetime(df_n['created_at_mse'])


# merge df_n med df_m_d_2025
df_merge = df_m_d.merge(df_n, left_on='synlig_for', right_on='personident', how='left')   


df_merge['har_status'] = df_merge['status'].notna()

status_summary = df_merge.groupby(['varighetsgruppe', 'har_status']).size().reset_index(name='antall')
status_summary['antall'].sum()
fig= px.bar(status_summary,
            x='varighetsgruppe',
            y='antall',
            color='har_status',
            barmode='group',
            text='antall',
            labels={
                'varighetsgruppe': 'Varighetsgruppe',
                'antall': 'Antall',
                'har_status': 'Har status'
            })
fig.update_layout(xaxis_title='Varighetsgruppe',
                  yaxis_title='Antall',
                  legend_title='Har status')
fig.show()

# %% [markdown]
#### Grunn for Dialogmøte synlighet i over 30 uker (2025)
### fordelling av status i Dialogmøte
#%% 

# 2. Filtrer df_m: Kun 2025 og tjeneste = DIALOGMOTE
#df_m['opprettet'] = pd.to_datetime(df_m['opprettet'])

# 3. Sørg for at statusdata er datetime
df_n_f['created_at_mse'] = pd.to_datetime(df_n_f['created_at_mse'])

# 4. Hent siste status per personident
df_n_f_latest = df_n_f.sort_values('created_at_mse').drop_duplicates('personident', keep='last')

# 5. Merge: df_m_d + siste status per person
df_merge = df_m_d.merge(
    df_n_f_latest,
    left_on='synlig_for',
    right_on='personident',
    how='left'
)

# 6. (Valgfritt) Erstatt NaN med "INGEN STATUS"
df_merge['status'] = df_merge['status'].fillna('INGEN STATUS')

# 7. Lag oppsummering: varighetsgruppe + status
status_summary = df_merge.groupby(['varighetsgruppe', 'status']).size().reset_index(name='antall')

# 8. Visualisering: Stacked bar chart
fig = px.bar(
    status_summary,
    x='varighetsgruppe',
    y='antall',
    color='status',
    barmode='stack',
    text='antall',
    labels={
        'varighetsgruppe': 'Varighetsgruppe',
        'antall': 'Antall',
        'status': 'Status'
    },
    category_orders={"varighetsgruppe": gruppe_rekkefølge}
)

fig.update_layout(
    xaxis_title='Varighetsgruppe',
    yaxis_title='Antall',
    legend_title='Status'
)

fig.show()


# %% [markdown]
# :::



# %% [markdown]
# :::

