
from os.path import isfile
from os import access, R_OK
import datetime as dt
import pandas as pd 
import numpy as np
from plotly import graph_objects as go
import plotly.express as px
import plotly.io as pio
pio.renderers.default = ("plotly_mimetype+" + "notebook_connected+" + "iframe_connected")
import random
import re 

def get_dict(filename):

    assert isfile(filename), f"File not found: '{filename}'"
    assert access(filename, R_OK), f"File '{filename}' doesn't exist or isn't readable"

    f = open(filename)

    content = f.read().splitlines() 

    f.close()

    isFindComment = True

    keys = []
    queries = []

    while len (content) > 0:

        c = content.pop(0)

        if len(c) == 0:
            continue

        isComment = c.startswith("/*") and c.endswith("*/")

        isSQL = c.endswith(';')

        if isComment and isFindComment:
            c = c[2:]
            c = c[:-2]

            query_string = c.split(':')

            sql_key = query_string[0]

            keys.append(sql_key)

            isFindComment = False

        elif isSQL and not isFindComment:
            c = c[:-1]
            queries.append(c)
            isFindComment = True
            
    if len(keys) > len (queries):
        keys.pop()

    assert len(keys) == len (queries)

    d = dict(zip(keys, queries))

    return d



def get_date_formats(df, timestamp_col):
   
    df['h'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%H')
    df['d'] = df[timestamp_col].astype('datetime64[ns]').dt.date
    df['dw'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%A')
    df['dm'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%d')
    df['yw'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%Y-%W')
    df['ym'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%Y-%m')
    
    return df



def get_dwmy_df(df, date_col='d', week_col='yw', month_col='ym'):
    
    g_day = df.groupby([df[date_col].dt.date]).size().reset_index(name="n_count")
    g_day = g_day.rename(columns={date_col: 'Tid'})

    g_week = df.groupby([df[week_col]]).size().reset_index(name="n_count").sort_values(by=week_col)
    g_week = g_week.rename(columns={week_col: 'Tid'})

    g_month = df.groupby([df[month_col]]).size().reset_index(name="n_count").sort_values(by=month_col)
    g_month = g_month.rename(columns={month_col: 'Tid'})

    g_year = df.groupby([df[month_col].str.slice(0,4)]).size().reset_index(name="n_count").sort_values(by=month_col)
    g_year = g_year.rename(columns={month_col: 'Tid'})

    return [(g_week, "Uke"), (g_month, "Måned"), (g_year, "År"), (g_day, "Dag")]



def dwm_bar_plot(t_g):

    n_traces_per_button = 1

    t_g_buttons = []

    fig = go.Figure()

    for i, (df_plot, knappenavn) in enumerate(t_g):

        visible = True if i == 0 else False
        
        fig.add_trace(
            go.Scatter(
                x = df_plot['Tid'],
                y = df_plot['n_count'],
                name="",
                visible = visible))
                    
        visible_button = np.array([False] * len(t_g) * n_traces_per_button)
        
        start = i * n_traces_per_button
        slutt = start + n_traces_per_button
        
        visible_button[start:slutt] = True
        t_g_buttons.append(dict(
            label = knappenavn,
            method = "update",
            args = [{"visible": visible_button}]
        ))
        
    updatemenus = list([
            dict(active = 0,
                showactive = True, 
                buttons = t_g_buttons,
                x = 0.15,
                direction = "right",
                y = 1.15,
                type = "buttons")
        ])

    fig.update_layout(updatemenus=updatemenus,
                    legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=1.10,
                    xanchor="left",
                    x=.75))

    fig.update_yaxes(title_text = "Antall")

    return fig.update_xaxes(dict(type="category"))





def list_UTC_columns(df):

    l_utc = list(df.select_dtypes('datetimetz').columns)

    return l_utc 


def utc_to_local(df):
    
    l_utc = list_UTC_columns(df)

    if l_utc:
        for col in l_utc:
            df[col] = df[col].apply(lambda x: x.tz_convert('Europe/Oslo').tz_localize(None))    
        return df

    else:
        return df 


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

#Funksjon for å lage Sankey-diagram fra hendelser
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


