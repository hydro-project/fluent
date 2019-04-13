import logging
import numpy as np
import scipy.stats

def mean_confidence_interval(data, confidence=0.95):
    n = len(data)
    m, se = np.mean(data), scipy.stats.sem(data)
    h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
    return m, m-h, m+h

def print_latency_stats(data, ident, log=False):
    npdata = np.array(data)
    interval = mean_confidence_interval(npdata)

    median = np.percentile(npdata, 50)
    p75 = np.percentile(npdata, 75)
    p95 = np.percentile(npdata, 95)
    p99 = np.percentile(npdata, 99)
    mx = np.max(npdata)

    p25 = np.percentile(npdata, 25)
    p05 = np.percentile(npdata, 5)
    p01 = np.percentile(npdata, 1)
    mn = np.min(npdata)

    output = ('%s LATENCY:\n\tmean: %.6f, median: %.6f\n\t95%% confidence: ' +\
            '(%.6f, %.6f)\n\tmin/max: (%.6f, %.6f)\n\tp25/p75: (%.6f, %.6f) ' + \
            '\n\tp5/p95: (%.6f, %.6f)\n\tp1/p99: (%.6f, %.6f)') % \
            (ident, interval[0], median, interval[1], interval[2], mn, mx, p25,
                    p75, p05, p95, p01, p99)

    if log:
        logging.info(output)
    else:
        print(output)
