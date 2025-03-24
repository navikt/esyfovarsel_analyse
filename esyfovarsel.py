
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
from tools import get_dict, get_date_formats
import plotly.express as px
import plotly.io as pio
pio.renderers.default = ("plotly_mimetype+" + "notebook_connected+" + "iframe_connected")


# %%
project = 'teamsykefravr-prod-7e29'
d_sql = get_dict("/home/user/esyfovarsel_analyse/esyfovarsel.sql")

# %%
#| echo: false
#| output: false
df = pandas_gbq.read_gbq(d_sql['esyfovarsel_alt'], project_id=project)
df['år'] = df.utsendt_tidspunkt.astype('datetime64[ns]').dt.strftime('%Y')
df = get_date_formats(df, "utsendt_tidspunkt")

# %% [markdown]

# :::{.column-page}

### Esyfovarsel

#### Antall typer sendte varsler per år

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

# Lag stolpediagram
fig = px.bar(gr2, x='år', y='antall')
fig.update_layout(xaxis=dict(type="category"))

# %% [markdown]

####  Antall Frekvensanalyse av Varsler per Type'
# %% 
# Gruppere etter 'type' og telle antall forekomster
gr3 = df['type'].value_counts().reset_index()
gr3.columns = ['type', 'antall']

# Lag stolpediagram
fig = px.bar(gr3, x='type', y='antall',  color='type', color_discrete_sequence=px.colors.qualitative.Dark24)
fig.update_layout(xaxis=dict(type="category"))

# Vis diagrammet
fig.show()

# %% [markdown]

####  Tidspunktanalyse per Time
# %%


# Gruppere etter time og telle antall forekomster
hourly_counts = df.groupby('h').size().reset_index(name='antall')

# Lag stolpediagram for timeanalyse
fig_hour = px.bar(hourly_counts, x='h', y='antall', color='h')
fig_hour.update_layout(xaxis=dict(type="category"))

# Vis diagrammet
fig_hour.show()


# %% [markdown]

####  Tidspunktanalyse per Ukedag
# %%
# Gruppere etter ukedag og telle antall forekomster
weekday_counts = df.groupby('dw').size().reset_index(name='antall')

# Sortere ukedagene i riktig rekkefølge
weekday_counts['dw'] = pd.Categorical(weekday_counts['dw'], categories=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ordered=True)
weekday_counts = weekday_counts.sort_values('dw')

# Lag stolpediagram for ukedagsanalyse
fig_weekday = px.bar(weekday_counts, x='dw', y='antall', color='dw', color_discrete_sequence=px.colors.qualitative.Dark24)
fig_weekday.update_layout(xaxis=dict(type="category"))

# Vis diagrammet
fig_weekday.show()

# %% [markdown]
# :::
