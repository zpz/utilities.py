from datetime import datetime

from bokeh.plotting import figure
import numpy


def hist(data, bins=20, fill_color="#D95B43", line_color="#033649",
        background_fill_color="#E8DDCB",  **fig_args):
    fig = figure(background_fill_color=background_fill_color, **fig_args)
    hist, edges = numpy.histogram(data, bins=bins)
    fig.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:], 
        fill_color=fill_color, line_color=line_color)
    return fig


def boxplot(data, background_fill_color="#EFE8E2", **fig_args):
    '''
    `data`: a dict of numpy arrays.
    '''
    labels = list(data.keys())
    values = list(data.values())

    q1 = numpy.array([numpy.quantile(v, q=0.25) for v in values])
    q2 = numpy.array([numpy.quantile(v, q=0.5) for v in values])
    q3 = numpy.array([numpy.quantile(v, q=0.75) for v in values])
    iqr = q3 - q1
    upper = q3 + 1.5*iqr
    lower = q1 - 1.5*iqr

    def outliers(x, lower, upper):
        x = x[(x > upper) | (x < lower)]
        return x[~numpy.isnan(x)]

    out = [outliers(x, l, u) for x, l, u in zip(values, lower, upper)]
    out_empty = sum(len(v) for v in out) == 0

    # prepare outlier data for plotting, we need coordinates for every outlier.
    if not out_empty:
        outx = []
        outy = []
        for x, label in zip(out, labels):
            if len(x) > 0:
                outx.extend([label] * len(x))
                outy.extend(x)

    # shrink lengths of stems to be no longer than the minimums or maximums
    qmin = [numpy.min(v) for v in values]
    qmax = [numpy.max(v) for v in values]
    lower = [max(a,b) for a,b in zip(lower, qmin)]
    upper = [min(a,b) for a,b in zip(upper, qmax)]

    x_datetime = isinstance(labels[0], datetime)
    x_unit = 3600000 if x_datetime else 1

    #args = {'background_fill_color': background_fill_color, 'x_range': labels, **figargs}
    args = {'background_fill_color': background_fill_color, **fig_args}
    if x_datetime:
        args['x_axis_type'] = 'datetime'
    else:
        args['x_range'] = labels
    fig = figure(**args)
    fig.xgrid.grid_line_color = None
    fig.ygrid.grid_line_color = "white"
    fig.grid.grid_line_width = 2
    fig.xaxis.major_label_text_font_size= "12pt"

    # stems
    fig.segment(labels, upper, labels, q3, line_color="black")
    fig.segment(labels, lower, labels, q1, line_color="black")

    # boxes
    fig.vbar(labels, 0.7*x_unit, q2, q3, fill_color="#E08E79", line_color="black")
    fig.vbar(labels, 0.7*x_unit, q1, q2, fill_color="#3B8686", line_color="black")

    # whiskers (almost-0 height rects simpler than segments)
    hgt = (numpy.max(upper) - numpy.min(lower)) / 100.
    fig.rect(labels, lower, 0.2*x_unit, hgt, fill_color='black', line_color="black")
    fig.rect(labels, upper, 0.2*x_unit, hgt, fill_color='black', line_color="black")

    # outliers
    if not out_empty:
        fig.circle(outx, outy, size=3, color="#F38630", fill_alpha=0.6)

    return fig