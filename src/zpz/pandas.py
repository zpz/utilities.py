import pandas as pd


default = {k: pd.get_option(k) for k in
    ['display.max_rows', 'display.max_columns', 'display.max_colwidth',
    'display.width', 'display.float_format']}


def set_pandas_full_display(max_rows=256, max_columns=20, max_colwidth=200, display_width=600):
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.max_columns', max_columns)
    pd.set_option('display.max_colwidth', max_colwidth)
    pd.set_option('display.width', display_width)
    pd.set_option('display.float_format', 
        lambda x: '{:.2f}'.format(x) if x >= 1. else '{:.2g}'.format(x))


def set_pandas_default_display():
    for k,v in default.items():
        pd.set_option(k, v)
