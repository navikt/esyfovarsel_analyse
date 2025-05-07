
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
import random



from tools import (
    get_dict, 
    get_date_formats, 
    get_dwmy_df,
    dwm_bar_plot,
    lag_hendelsesflyt_og_beregn_tid,
    lag_sankey_fra_hendelser,
    finn_vanlige_sekvenser,
    vis_overgang_heatmap
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

# alle dialogmote isyfo
df_d = pandas_gbq.read_gbq(d_sql['isyfo_mote_status_endret'],project_id=project)
df_d = get_date_formats(df_d, "created_at")

# alle mikrofrontend-synlighet
df_s = pandas_gbq.read_gbq(d_sql['esyfovarsel_mikrofrontend_synlighet'],project_id=project)
df_s = get_date_formats(df_s, "opprettet")




# %% [markdown]

# :::{.column-page}

### Utsendte varsler

# ::: {.panel-tabset}

#### Antall utsendte varsler totalt

#%%
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
#### Datakildesammenligning: INNKALT vs SM_DIALOGMOTE_INNKALT

# %%
df_innkalt_dialog = df_d[df_d['status'] == 'INNKALT'].groupby('yw')
df_sm_dialog = df[df['type'] == 'SM_DIALOGMOTE_INNKALT'].groupby('yw')

row_counts = pd.DataFrame({
    'Kilde': ['INNKALT', 'SM_DIALOGMOTE_INNKALT'],
    'Antall': [len(df_innkalt_dialog), len(df_sm_dialog)]
})

fig = px.bar(row_counts, x='Kilde', y='Antall')
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
#### Flyt mellom Kalendertilstand (grupperingsid)
# %%
## uten syklus og uten mindre hendelse 
# Bruk funksjonen til å beregne hendelsesflyt og tid
df_final = lag_hendelsesflyt_og_beregn_tid(df_k)

# Lag Sankey-diagram fra de beregnede dataene
lag_sankey_fra_hendelser(df_final)

# %% [markdown]
#### Antall per Siste Kalendertilstand basert på grupperingsid
#%%
# Telling av siste tilstand
siste_tilstand_count = df_final['siste_tilstand'].value_counts().reset_index()
siste_tilstand_count.columns = ['siste_tilstand', 'count']

# Lag en søylediagram for siste hendelsestilstand
fig = px.bar(siste_tilstand_count, x='siste_tilstand', y='count',
             #title="Antall per Siste Kalendertilstand basert på kalenderid",
             labels={'siste_tilstand': 'Kalendertilstand', 'count': 'Antall Kalenderid'},
             color='siste_tilstand', color_discrete_sequence=px.colors.qualitative.Set2)

# Vis diagrammet
fig.show()

# %% [markdown]
#### Heatmap for vanligste-sekvenser i hendelse flyt
# %%

# Finn de vanligste sekvensene mellom hendelser i grupperingsid
vanligste_sekvenser = finn_vanlige_sekvenser(df_final, top_n=10)
# Lag heatmap for overganger mellom hendelser med vanligste sekvenser
vis_overgang_heatmap(vanligste_sekvenser)

# %% [markdown]
#### Flyt mellom Kalendertilstand (kalenderid)

#%%
# Funksjon for å beregne hendelsesflyt og tid
def lag_hendelsesflyt_og_beregn_tid1(df):
    records = []
    time_results = []
    sakid_results = []

    for group_id, group in df.groupby('kalenderid'):
        group = group.sort_values(by='opprettet')
        hendelser = group['kalenderavtaletilstand'].tolist()
        opprettede_tidspunkter = group['opprettet'].tolist()

        if len(hendelser) == 0:
            continue

        sakid = group['sakid'].iloc[0] if group['sakid'].nunique() == 1 else None
        record = {
            'kalenderid': group_id,
            'sakid': sakid
        }

        for i, (h, t) in enumerate(zip(hendelser, opprettede_tidspunkter), start=1):
            record[f'{i}_hendelse'] = h
            record[f'{i}_tidspunkt'] = t

        records.append(record)

        # Tidsberegning
        total_time = 0
        if len(group) == 1:
            total_time += (group['starttidspunkt'].iloc[0] - group['opprettet'].iloc[0]).total_seconds() / 3600
        else:
            total_time += (group['starttidspunkt'].iloc[0] - group['opprettet'].iloc[0]).total_seconds() / 3600
            for i in range(1, len(group)):
                total_time += (group['starttidspunkt'].iloc[i] - group['opprettet'].iloc[0]).total_seconds() / 3600

        if total_time >= 24:
            time_results.append({
                'kalenderid': group_id,
                'total_tid_brukt': total_time / 24,
                'brukte_dager': True 
            })
        else:
            time_results.append({
                'kalenderid': group_id,
                'total_tid_brukt': total_time,
                'brukte_dager': False
            })

        valid_events_count = sum(pd.notna(group['kalenderavtaletilstand']).astype(int))
        sakid_results.append({
            'kalenderid': group_id,
            'antall_hendelser_med_verdi': valid_events_count
        })

    df_hendelsesflyt = pd.DataFrame(records)
    df_total_tid = pd.DataFrame(time_results)
    df_sakid = pd.DataFrame(sakid_results)

    df_final = pd.merge(df_hendelsesflyt, df_total_tid, on='kalenderid', how='left')
    df_final = pd.merge(df_final, df_sakid, on='kalenderid', how='left')


    def get_siste_tilstand(row):
        hendelser = [row[col] for col in df_final.columns if col.endswith('_hendelse')]
        siste_tilstand = next((h for h in reversed(hendelser) if pd.notna(h)), None)
        return siste_tilstand

    df_final['siste_tilstand'] = df_final.apply(get_siste_tilstand, axis=1)

    return df_final


# Funksjon for å lage Sankey-diagram fra hendelser
def lag_sankey_fra_hendelser1(df_final):
    # Hent kolonner som inneholder hendelser i rekkefølge


    
    hendelse_kolonner = sorted([col for col in df_final.columns if col.endswith('_hendelse')],
                               key=lambda x: int(x.split('_')[0]))

    # Lag sekvenser av hendelser per gruppingsid uten direkte gjentakelser
    flyt_lister = []
    for _, row in df_final.iterrows():
        sekvens = [row[col] for col in hendelse_kolonner if pd.notna(row[col])]
        filtrert_sekvens = []

        for hendelse in sekvens:
            if len(filtrert_sekvens) == 0 or hendelse != filtrert_sekvens[-1]:
                filtrert_sekvens.append(hendelse)

        if len(filtrert_sekvens) >= 2:
            for i in range(len(filtrert_sekvens) - 1):
                flyt_lister.append((filtrert_sekvens[i], filtrert_sekvens[i+1]))

    # Telle unike overganger
    overgangsteller = pd.Series(flyt_lister).value_counts().reset_index()
    overgangsteller.columns = ['source_target', 'count']
    overgangsteller[['source', 'target']] = pd.DataFrame(overgangsteller['source_target'].tolist(), index=overgangsteller.index)
    
    # Filtrer ut flyt med verdi under 5
    overgangsteller = overgangsteller[overgangsteller['count'] >= 100]

    # Unike noder
    unike_noder = pd.unique(overgangsteller[['source', 'target']].values.ravel())
    node_map = {k: i for i, k in enumerate(unike_noder)}

    # Sankey-data
    sources = overgangsteller['source'].map(node_map)
    targets = overgangsteller['target'].map(node_map)
    values = overgangsteller['count']

    # Funksjon for å generere tilfeldige farger
    def random_color():
        r = lambda: random.randint(100, 255)
        return f'rgba({r()},{r()},{r()},0.5)'

    # Lag en farge for hver overgang
    link_colors = [random_color() for _ in range(len(overgangsteller))]
    
    fig = go.Figure(data=[go.Sankey(
        arrangement="perpendicular",
        node=dict(
            pad=20,
            thickness=30,
            line=dict(color="black", width=0.8),
            label=unike_noder.tolist(),
            color="lightblue"
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=link_colors  # Ulike farger for hver link
        ))])

    fig.update_layout(
        #title_text="Flyt mellom hendelser med kalenderid (uten direkte syklus i hendelser)",
        font=dict(size=14),
        height=800
    )
    fig.show()


df_final_1 = lag_hendelsesflyt_og_beregn_tid1(df_k)

# Lag Sankey-diagram fra de beregnede dataene
lag_sankey_fra_hendelser1(df_final_1)
# %% [markdown]
#### Antall per Siste Kalendertilstand basert på kalenderid
#%%
# Telling av siste tilstand
siste_tilstand_count = df_final_1['siste_tilstand'].value_counts().reset_index()
siste_tilstand_count.columns = ['siste_tilstand', 'count']

# Lag en søylediagram for siste hendelsestilstand
fig = px.bar(siste_tilstand_count, x='siste_tilstand', y='count',
             #title="Antall per Siste Kalendertilstand basert på kalenderid",
             labels={'siste_tilstand': 'Kalendertilstand', 'count': 'Antall Kalenderid'},
             color='siste_tilstand', color_discrete_sequence=px.colors.qualitative.Set2)

# Vis diagrammet
fig.show()

# %% [markdown]
# :::
# %% [markdown]
# :::


# %%
