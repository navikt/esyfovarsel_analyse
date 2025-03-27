
from os.path import isfile
from os import access, R_OK
import datetime as dt
import pandas as pd 
import numpy as np
from plotly import graph_objects as go
import plotly.express as px
import plotly.io as pio
pio.renderers.default = ("plotly_mimetype+" + "notebook_connected+" + "iframe_connected")


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