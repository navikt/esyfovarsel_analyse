
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
import pandas_gbq 
import sys
import re
import os
from tools import (
    get_dict, 
    get_date_formats, 
    get_dwmy_df,
    dwm_bar_plot
)

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

# %% [markdown]

# :::{.column-page}

### Utsendte varsler

# ::: {.panel-tabset}

#### Antall utsendte varsler per år

#%%
gr1 = df.groupby("år").type.value_counts().reset_index(name = 'antall')

# Lag stolpediagram
fig = px.bar(gr1, x='år', y='antall', color='type',color_discrete_sequence = px.colors.qualitative.Dark24)
fig.update_layout(xaxis=dict(type="category"))

# %% [markdown]

####  Antall varsle melding for type 'SM_DIALOGMOTE_SVAR_MOTEBEHOV' per år
# %% 

# Filtrere DataFrame for en bestemt type
type_df = df[df['type'] == 'SM_DIALOGMOTE_SVAR_MOTEBEHOV']

# Gruppere etter 'år' og telle antall forekomster
gr2 = type_df.groupby('år').size().reset_index(name='antall')

#fig = px.bar(gr, x="kalenderavtaletilstand", y="total_tid_brukt_dager",color="kalenderavtaletilstand")


# Lag stolpediagram
fig = px.bar(gr2, x='år', y='antall', color="år",color_discrete_sequence=['red', 'green', 'blue'] )
fig.update_layout(xaxis=dict(title="år"),
                  yaxis=dict(title="Antall"),
                  width=1000,
                  showlegend=False
                  )
#fig.update_layout(xaxis=dict(type="category"))

# %% [markdown]

####  Antall Frekvensanalyse av Varsler per Type'
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

####  Antall varsler per time
# %%

# Gruppere etter time og telle antall forekomster
hourly_counts = df.groupby('h').size().reset_index(name='antall')


# Lag stolpediagram for timeanalyse
fig_hour = px.bar(hourly_counts, x='h', y='antall', color='h')
#fig_hour.update_layout(xaxis=dict(type="category"))
fig_hour.update_layout(xaxis=dict(title="Tid"),
                  yaxis=dict(title="Antall"),
                  width=1000,
                  showlegend=False
                  )

# Vis diagrammet
fig_hour.show()


# %% [markdown]

####  Antall varsler per ukedag
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
#### Arbeidsgiverens tidsbruk(snitt)
# %%


df_k['tid_brukt'] = df_k['starttidspunkt'] - df_k['opprettet']

df_k['tid_brukt'] = df_k['tid_brukt'].fillna(pd.Timedelta(0))

df_k_filtered = df_k[~df_k['kalenderavtaletilstand'].isin(['AVHOLDT', 'VENTER_SVAR_FRA_ARBEIDSGIVER'])]



gr = df_k_filtered.groupby('kalenderavtaletilstand')['tid_brukt'].mean().reset_index(name='total_tid_brukt')

gr['total_tid_brukt_dager'] = gr['total_tid_brukt'].dt.total_seconds() / 86400


fig = px.bar(gr, x="kalenderavtaletilstand", y="total_tid_brukt_dager",color="kalenderavtaletilstand")

fig.update_layout(xaxis=dict(title="Kalendertilstand"),
                  yaxis=dict(title="Antall(snitt)"),
                  width=1000)


# Vis plot
fig.show()

# %% [markdown]
# :::
# %% [markdown]
# :::

