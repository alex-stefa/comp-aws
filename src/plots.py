#!/usr/bin/python2.7

from __future__ import print_function
from collections import defaultdict
import sys
import os
import json
import pickle
import wx
import resource
import time
import xmlrpclib


data_dirs = ['data0/', 'data/']
out_dir = 'plots/'
pickle_blob = 'pickles.dat'
plhosts_file = 'plhosts.json'

global plhosts_info # init in __main__


#---------------------------------------------------------------------------------------------------------------------
# Filters
#---------------------------------------------------------------------------------------------------------------------

def remove_failure(ec2host):
    return ec2host != 'ec2-54-242-108-170.compute-1.amazonaws.com'

data_gap_begin = time.mktime(time.strptime("19-10-2012 17:00", "%d-%m-%Y %H:%M"))
data_gap_end = time.mktime(time.strptime("19-10-2012 22:00", "%d-%m-%Y %H:%M"))
def remove_datagap(timestamp):
    return (timestamp < data_gap_begin) or (timestamp > data_gap_end)

time_remap_0 = time.mktime(time.strptime("04-01-1999 00:00", "%d-%m-%Y %H:%M")) # Monday
def week_remap(timestamp):
    time_struct = time.localtime(timestamp)
    return time_remap_0 + time_struct.tm_wday * 24*3600 + time_struct.tm_hour * 3600 + time_struct.tm_min * 60
def day_remap(timestamp):
    time_struct = time.localtime(timestamp)
    return time_remap_0 + time_struct.tm_hour * 3600 + time_struct.tm_min * 60 - 12 * 3600

def area_mapper(plhost):
    lat, long = plhosts_info[plhost]['latitude'], plhosts_info[plhost]['longitude']
    if lat > 22 and long < -30:
        return 'North America'
    if lat <= 22 and long < -30:
        return 'South America'
    if lat > 30 and -30 < long < 40:
        return 'Europe'
    if lat <= 30 and -30 < long < 40:
        return 'Africa'
    if long >= 40:
        return 'Asia'

def plarea_filter(accepted_areas):
    return (lambda plhost: area_mapper(plhost) in accepted_areas)


#---------------------------------------------------------------------------------------------------------------------
# Params
#---------------------------------------------------------------------------------------------------------------------

costs_micro = {
    'compute-1.amazonaws.com': 0.065,
    'us-west-2.compute.amazonaws.com': 0.065,
    'us-west-1.compute.amazonaws.com': 0.090,
    'eu-west-1.compute.amazonaws.com': 0.085,
    'ap-southeast-1.compute.amazonaws.com': 0.085,
    'ap-northeast-1.compute.amazonaws.com': 0.092,
    'sa-east-1.compute.amazonaws.com': 0.115
}

costs_large = {
    'compute-1.amazonaws.com': 0.520,
    'us-west-2.compute.amazonaws.com': 0.520,
    'us-west-1.compute.amazonaws.com': 0.720,
    'eu-west-1.compute.amazonaws.com': 0.680,
    'ap-southeast-1.compute.amazonaws.com': 0.680,
    'ap-northeast-1.compute.amazonaws.com': 0.736,
    'sa-east-1.compute.amazonaws.com': 0.920
}

even_distrib = {
    'North America': 0.25,
    'South America': 0.25,
    'Europe': 0.25,
    'Asia': 0.25
}

asian_distrib = {
    'North America': 0.15,
    'South America': 0.05,
    'Europe': 0.10,
    'Asia': 0.70
}

full_timeline_params = {
    'type': 'LINE',
    'step': 60*60*1,
    'interval': 60*60*3,
    'dotpitch': 7,
    'zeromin': False,
    'timeremap': None,
    'distrib': None
}

week_remap_params = {
    'type': 'LINE',
    'step': 60*60*1,
    'interval': 60*60*3,
    'dotpitch': 7,
    'zeromin': False,
    'timeremap': week_remap,
    'distrib': None
}

day_remap_params = {
    'type': 'LINE',
    'step': 60*15,
    'interval': 60*45,
    'dotpitch': 10,
    'zeromin': False,
    'timeremap': day_remap,
    'distrib': None
}

even_timeline_params = {
    'type': 'LINE',
    'step': 60*60*1,
    'interval': 60*60*3,
    'dotpitch': 7,
    'zeromin': False,
    'timeremap': None,
    'distrib': even_distrib
}

asian_timeline_params = {
    'type': 'LINE',
    'step': 60*60*1,
    'interval': 60*60*3,
    'dotpitch': 7,
    'zeromin': False,
    'timeremap': None,
    'distrib': asian_distrib
}

even_day_remap_params = {
    'type': 'LINE',
    'step': 60*15,
    'interval': 60*45,
    'dotpitch': 10,
    'zeromin': False,
    'timeremap': day_remap,
    'distrib': even_distrib
}

asian_day_remap_params = {
    'type': 'LINE',
    'step': 60*15,
    'interval': 60*45,
    'dotpitch': 10,
    'zeromin': False,
    'timeremap': day_remap,
    'distrib': asian_distrib
}

def cost_large_asian_params(max_cost):
    return {
    'type': 'LINE',
    'step': 60*60*3,
    'interval': 60*60*3,
    'dotpitch': 15,
    'zeromin': False,
    'timeremap': week_remap,
    'distrib': asian_distrib,
    'costs': costs_large,
    'maxcost': max_cost
}

rank_params = {
    'type': 'RANK',
    'step': 60*60*1,
    'interval': 60*60*3
}

cdf_params = {
    'type': 'CDF',
    'range': [0.01, 0.93],
    'steps': 100
}

disc_params = {
    'type': 'DISC'

}


#---------------------------------------------------------------------------------------------------------------------
# Plots
#---------------------------------------------------------------------------------------------------------------------

plots = [
    {
        'filename': 'timeline-file1k-1h-all',
        'title': 'Most responsive EC2 instance for file1k',
        'description': 'using all PlanetLab nodes',
        'h_name': 'Time (Oct 15 - Nov 5)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': full_timeline_params,
        'request': '/requests/file1k'
    },
    {
        'filename': 'timeline-file128k-1h-all',
        'title': 'Most responsive EC2 instance for file128k',
        'description': 'using all PlanetLab nodes',
        'h_name': 'Time (Oct 15 - Nov 5)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': full_timeline_params,
        'request': '/requests/file128k'
    },
    {
        'filename': 'week-file1k-1h-all',
        'title': 'Most responsive EC2 instance for file1k',
        'description': 'average week - using all PlanetLab nodes',
        'h_name': 'Time (one week average)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': week_remap_params,
        'request': '/requests/file1k'
    },
   {
        'filename': 'week-file128k-1h-all',
        'title': 'Most responsive EC2 instance for file128k',
        'description': 'average week - using all PlanetLab nodes',
        'h_name': 'Time (one week average)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': week_remap_params,
        'request': '/requests/file128k'
    },
    {
        'filename': 'day-file1k-15m-all',
        'title': 'Most responsive EC2 instance for file1k',
        'description': 'average day - using all PlanetLab nodes',
        'h_name': 'Time (one day average)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': day_remap_params,
        'request': '/requests/file1k'
    },
    {
        'filename': 'day-file128k-15m-all',
        'title': 'Most responsive EC2 instance for file128k',
        'description': 'average day - using all PlanetLab nodes',
        'h_name': 'Time (one day average)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': day_remap_params,
        'request': '/requests/file128k'
    },
    {
        'filename': 'cdf-file1k-all',
        'title': 'Distribution of response times for file1k',
        'description': 'using all PlanetLab nodes',
        'h_name': 'Response time (s)',
        'v_name': 'Percentage of all requests',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': cdf_params,
        'request': '/requests/file1k'
    },
    {
        'filename': 'cdf-file128k-all',
        'title': 'Distribution of response times for file128k',
        'description': 'using all PlanetLab nodes',
        'h_name': 'Response time (s)',
        'v_name': 'Percentage of all requests',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': cdf_params,
        'request': '/requests/file128k'
    },
]

for area in [
        ('North America', 'na'),
        ('Europe', 'eu'),
        ('Asia', 'as')
    ]:
    plots += [
    {
        'filename': 'timeline-file1k-1h-%s' % area[1],
        'title': 'Most responsive EC2 instance for file1k',
        'description': 'using PlanetLab nodes in %s' % area[0],
        'h_name': 'Time (Oct 15 - Nov 5)',
        'v_name': 'Average response times (s)',
        'plfilter': [plarea_filter([area[0]])],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': full_timeline_params,
        'request': '/requests/file1k'
    },
    {
        'filename': 'timeline-file128k-1h-%s' % area[1],
        'title': 'Most responsive EC2 instance for file128k',
        'description': 'using PlanetLab nodes in %s' % area[0],
        'h_name': 'Time (Oct 15 - Nov 5)',
        'v_name': 'Average response times (s)',
        'plfilter': [plarea_filter([area[0]])],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': full_timeline_params,
        'request': '/requests/file128k'
    },
    {
        'filename': 'cdf-file1k-%s' % area[1],
        'title': 'Distribution of response times for file1k',
        'description': 'using PlanetLab nodes in %s' % area[0],
        'h_name': 'Response time (s)',
        'v_name': 'Percentage of all requests',
        'plfilter': [plarea_filter([area[0]])],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': cdf_params,
        'request': '/requests/file1k'
    },
    {
        'filename': 'cdf-file128k-%s' % area[1],
        'title': 'Distribution of response times for file128k',
        'description': 'using PlanetLab nodes in %s' % area[0],
        'h_name': 'Response time (s)',
        'v_name': 'Percentage of all requests',
        'plfilter': [plarea_filter([area[0]])],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': cdf_params,
        'request': '/requests/file128k'
    }
]

for distrib in [
        ('even', 'an even area', even_timeline_params),
        ('asian', 'a predominant Asian', asian_timeline_params)
    ]:
    plots += [
    {
        'filename': 'timeline-file1k-1h-[%s]' % distrib[0],
        'title': 'Most responsive EC2 instance for file1k',
        'description': 'using %s PlanetLab nodes distribution accross continents' % distrib[1],
        'h_name': 'Time (Oct 15 - Nov 5)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': distrib[2],
        'request': '/requests/file1k'
    },
    {
        'filename': 'timeline-file128k-1h-[%s]' % distrib[0],
        'title': 'Most responsive EC2 instance for file128k',
        'description': 'using %s PlanetLab nodes distribution accross continents' % distrib[1],
        'h_name': 'Time (Oct 15 - Nov 5)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': distrib[2],
        'request': '/requests/file128k'
    }
]

for distrib in [
        ('even', 'an even area', even_day_remap_params),
        ('Asian', 'a predominant Asian', asian_day_remap_params)
    ]:
    plots += [
    {
        'filename': 'day-file1k-15m-[%s]' % distrib[0],
        'title': 'Most responsive EC2 instance for file1k',
        'description': 'average day - using %s PlanetLab nodes distribution accross continents' % distrib[1],
        'h_name': 'Time (one day average)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': distrib[2],
        'request': '/requests/file1k'
    },
    {
        'filename': 'day-file128k-15m-[%s]' % distrib[0],
        'title': 'Most responsive EC2 instance for file128k',
        'description': 'average day - using %s PlanetLab nodes distribution accross continents' % distrib[1],
        'h_name': 'Time (one day average)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': distrib[2],
        'request': '/requests/file128k'
    }
]

plots = []

for max_cost in range(85, 130, 5):
    plots += [
    {
        'filename': 'cost-file128k-3h-(%.1f-large)-[asian]' % max_cost,
        'title': 'Location plan for large EC2 instance for file128k',
        'description': 'max cost $%.1f using a predominant Asian PlanetLab nodes distribution accross continents' % max_cost,
        'h_name': 'Time (one predicted week)',
        'v_name': 'Average response times (s)',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [remove_datagap],
        'valuefilter': [],
        'params': cost_large_asian_params(max_cost),
        'request': '/requests/file128k'
    }
]

csv_dump = [
    {
        'filename': 'file1k',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [],
        'valuefilter': [],
        'request': '/requests/file1k'
    },
    {
        'filename': 'file128k',
        'plfilter': [],
        'ec2filter': [remove_failure],
        'timefilter': [],
        'valuefilter': [],
        'request': '/requests/file128k'
    }
]
csv_dump = []


#---------------------------------------------------------------------------------------------------------------------
# PlanetLab host info
#---------------------------------------------------------------------------------------------------------------------

def get_plhosts_info():
    plc_api = xmlrpclib.ServerProxy('https://www.planet-lab.org/PLCAPI/', allow_none=True)

    auth = {
        'AuthMethod' : 'password',
        'Username' : 'username@gmail.com',
        'AuthString' : 'p@ssw0rd'
    }
    slice_name='rice_comp529'

    node_ids = plc_api.GetSlices(auth, slice_name, ['node_ids'])[0]['node_ids']
    node_props = plc_api.GetNodes(auth, node_ids, ['hostname', 'site_id'])
    site_ids = set([node['site_id'] for node in node_props])
    site_props = plc_api.GetSites(auth, list(site_ids), ['site_id', 'name', 'abbreviated_name', 'longitude', 'latitude'])
    site_info = dict()
    for site in site_props:
        site_info[site['site_id']] = site
    node_info = dict()
    for node in node_props:
        node_info[node['hostname']] = site_info[node['site_id']]
    return node_info


def store_plhosts_info(info, filename):
    print('\n--- Storing PL host info to %s..' % filename)
    with open(filename, 'w') as f:
        json.dump(info, f)
    print('    Done! [file size: %d KB]' % (os.path.getsize(filename) // 1024))


def load_plhosts_info(filename):
    print('\n--- Loading PL host info from %s..' % filename)
    with open(filename, 'r') as f:
        info = json.load(f)
    print('    Done! [mem usage: %d KB]' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return info


def compute_plhosts_stats(plhosts):
    stats = defaultdict(int)
    for host in plhosts:
        stats[area_mapper(host)] += 1
    return stats


#---------------------------------------------------------------------------------------------------------------------
# Parsing
#---------------------------------------------------------------------------------------------------------------------

def collect_data(data_folder_list):
    all_data = defaultdict(dict)
    plhosts_list = list()
    plhosts_dict = dict()

    for data_folder in data_folder_list:
        print('\n--- Collecting data from %s..' % data_folder)
        for filename in os.listdir(data_folder):
            fullname = os.path.join(data_folder, filename)
            print('Reading %s..  [mem usage: %d KB]' % (fullname, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
            plhost = os.path.splitext(filename)[0]
            with open(fullname, 'r') as f:
                line_nr = 0
                for line in f:
                    line_nr += 1
                    if line_nr <= 3:
                        continue
                    record = json.loads(line.strip())
                    for ec2host, record_data in record.items():
                        for request, timing_value in record_data['times'].items():
                            if ec2host not in all_data[request]:
                                all_data[request][ec2host] = defaultdict(list)
                            minute = int(timing_value[0]) // 60 * 60
                            all_data[request][ec2host][minute].append((plhost, timing_value[2]))

    print('\n--- Sorting times..')
    all_data_sorted = dict()
    for request, time_records in all_data.items():
        all_data_sorted[request] = dict()
        for ec2host, records in time_records.items():
            timestamps = sorted(list(records.keys()))
            all_data_sorted[request][ec2host] = [(timestamp, records[timestamp]) for timestamp in timestamps]

    return all_data_sorted


def ensure_path(path):
    """ creates directory tree if it does not exist """
    try:
        os.makedirs(path)
    except:
        pass


def store_data(data, filename):
    print('\n--- Storing data to %s..' % filename)
    with open(filename, 'wb') as f:
        pickle.dump(data, f)
    print('    Done! [file size: %d KB]' % (os.path.getsize(filename) // 1024))


def load_data(filename):
    print('\n--- Loading data from %s..' % filename)
    with open(filename, 'rb') as f:
        data = pickle.load(f)
    print('    Done! [mem usage: %d KB]' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return data


def admit_one(filters, elem):
    if filters is None:
        return True
    for pred in filters:
        if not pred(elem):
            return False
    return True


def filter_data(data, plot_desc):
    print('\n--- Filtering data..')
    assert plot_desc['request'] is not None
    to_filter = data[plot_desc['request']]
    plhosts = set()
    filtered = dict()
    for ec2host, records in to_filter.items():
        if admit_one(plot_desc['ec2filter'], ec2host):
            filtered[ec2host] = list()
            for time_records in to_filter[ec2host]:
                if admit_one(plot_desc['timefilter'], time_records[0]):
                    acc_times = list()
                    for time in time_records[1]:
                        if (admit_one(plot_desc['plfilter'], time[0]) and
                            admit_one(plot_desc['valuefilter'], time[1])):
                            plhosts.add(time[0])
                            acc_times.append(time)
                    if len(acc_times) > 0:
                        filtered[ec2host].append((time_records[0], acc_times))

    return filtered, plhosts


def remap_time(data, time_map):
    if time_map is None:
        return data
    for ec2host, records in data.items():
        time_dict = defaultdict(list)
        for record in records:
            time_dict[time_map(record[0])] += record[1]
        data[ec2host] = sorted(time_dict.items(), key=lambda x: x[0])
    return data


def remap_plhosts(data, plhost_map):
    if plhost_map is None:
        return data
    for ec2host, records in data.items():
        for record in records:
            for entry in record[1]:
                entry[0] = plhost_map(entry[0])
    return data


#---------------------------------------------------------------------------------------------------------------------
# Data analysis
#---------------------------------------------------------------------------------------------------------------------

def plot(records, plhosts, plot_desc):
    if plot_desc['params']['type'] == 'LINE':
        do_plot_timeline(records, plot_desc)

    if plot_desc['params']['type'] == 'RANK':
        pass

    if plot_desc['params']['type'] == 'CDF':
        do_plot_cdf(records, plot_desc)

    if plot_desc['params']['type'] == 'DISC':
        pass


def time_str(timestamp, weekday=True, date=True, hour=True):
    time_struct = time.gmtime(timestamp + 6 * 3600) # avoid DST issues
    str = ''
    if weekday:
        str += time.strftime(' %a', time_struct)
    if date:
        str += time.strftime(' %d %b', time_struct)
    if hour:
        str += time.strftime(' %H:%M', time_struct)
    return str[1:]

"""
def get_avg_response(records, time, interval):
    if time < records[0][0] or time > records[-1][0]:
        return 0, 0
    sum_time = 0
    count_time = 0
    half_int = float(interval) / 2
    min_bound = 0
    max_bound = len(records) - 1
    mid_bound = None
    while max_bound >= min_bound:
        mid_bound = (min_bound + max_bound) // 2
        if records[mid_bound][0] < time:
            min_bound = mid_bound + 1
        elif records[mid_bound][0] > time:
            max_bound = mid_bound - 1
        else:
            break
    min_bound = mid_bound
    while min_bound >= 0 and records[min_bound][0] >= (time - half_int):
        min_bound -= 1
    max_bound = mid_bound
    while max_bound < len(records) and records[max_bound][0] <= (time + half_int):
        max_bound += 1
    for i in range(min_bound + 1, max_bound):
        if (time - half_int) <= records[i][0] <= (time + half_int):
            sum_time += sum(timing[1] for timing in records[i][1])
            count_time += len(records[i][1])
    if count_time == 0:
        return 0, 0
    return sum_time / float(count_time), count_time
"""

def get_avg_response(records, time, interval, distrib=None):
    if time < records[0][0] or time > records[-1][0]:
        return 0, 0
    sum_time = 0
    count_time = 0
    half_int = float(interval) / 2
    min_bound = 0
    max_bound = len(records) - 1
    mid_bound = None
    while max_bound >= min_bound:
        mid_bound = (min_bound + max_bound) // 2
        if records[mid_bound][0] < time:
            min_bound = mid_bound + 1
        elif records[mid_bound][0] > time:
            max_bound = mid_bound - 1
        else:
            break
    min_bound = mid_bound
    while min_bound >= 0 and records[min_bound][0] >= (time - half_int):
        min_bound -= 1
    max_bound = mid_bound
    while max_bound < len(records) and records[max_bound][0] <= (time + half_int):
        max_bound += 1
    totals = dict()
    for i in range(min_bound + 1, max_bound):
        if (time - half_int) <= records[i][0] <= (time + half_int):
            for timing in records[i][1]:
                pl_area = area_mapper(timing[0])
                sum_area, count_area = totals.get(pl_area, (0, 0))
                totals[pl_area] = (sum_area + timing[1], count_area + 1)
    w_avg = 0
    count_time = 0
    for area, p_total in totals.items():
        if distrib is not None and area not in distrib:
            continue
        if distrib is None:
            w_avg += p_total[0]
        else:
            w_avg += float(p_total[0]) / p_total[1] * distrib[area]
        count_time += p_total[1]
    if count_time == 0:
        return 0, 0
    if distrib is None:
        w_avg /= count_time
    else:
        w_avg /= float(sum(distrib.values())) # should be 1
    return w_avg, count_time


def get_ranking_timeline(times, plot_desc, valid_count=50):
    print('\n--- Computing timeline..')
    min_time = min(val[0][0] for val in times.values())
    max_time = max(val[-1][0] for val in times.values())
    timeline = defaultdict(list)
    min_val = None
    max_val = None

    for time in range(int(min_time), int(max_time), plot_desc['params']['step']):
        data_point = list()
        for ec2host, records in times.items():
            avg_time, count_time = get_avg_response(records, time, plot_desc['params']['interval'], plot_desc['params']['distrib'])
            if count_time < valid_count:
                timeline[ec2host].append((None, None, count_time))
            else:
                data_point.append((ec2host, avg_time, count_time))
                if max_val is None or avg_time > max_val:
                    max_val = avg_time
                if min_val is None or avg_time < min_val:
                    min_val = avg_time
        data_point = sorted(data_point, key=lambda x: x[1])
        for i, entry in enumerate(data_point):
            timeline[entry[0]].append((i, entry[1], entry[2]))

    return timeline, min_time, max_time, min_val, max_val


def get_cdf(records, plot_desc):
    print('\n--- Computing CDF..')
    cdf_data = dict()
    time_values = dict()

    for ec2host, time_records in records.items():
        values = list()
        for timestamp, request_times in time_records:
            for timing in request_times:
                values.append(timing[1])
        values = sorted(values)
        time_values[ec2host] = values

    range_min = plot_desc['params']['range'][0]
    range_max = plot_desc['params']['range'][1]
    min_val = min(v[int(range_min * len(v))] for v in time_values.values())
    max_val = max(v[int(range_max * len(v))-1] for v in time_values.values())
    step_val = (max_val - min_val) / float(plot_desc['params']['steps'])

    for ec2host, values in time_values.items():
        total_count = len(values)
        cdf_vals = list()
        next_val = min_val
        curr_count = 0
        while next_val <= max_val:
            while values[curr_count] <= next_val:
                curr_count += 1
            cdf_vals.append(float(curr_count-1)/total_count)
            next_val += step_val
        cdf_data[ec2host] = cdf_vals
    return cdf_data, min_val, max_val


def get_location_plan(timeline, plot_desc):
    nr_points = len(timeline[timeline.keys()[0]]) # assume all lengths equal
    hosts = sorted_ec2hosts(timeline.keys())
    max_cost = plot_desc['params']['maxcost']
    cost_map = plot_desc['params']['costs']
    step_h = plot_desc['params']['step'] / float(3600)
    fast_cost = 0
    for ec2host, times in timeline.items():
        for pt in times:
            if pt[0] == 0: # rank 1
                fast_cost += lookup_cost(ec2host, cost_map) * step_h
    print('fast_cost=%.3f max_cost=%.3f' % (fast_cost, max_cost))

    loc = [''] * nr_points
    for ec2host, times in timeline.items():
        for i, pt in enumerate(times):
            if pt[0] == 0: # rank 1
                loc[i] = ec2host

    if max_cost >= fast_cost:
        return loc

    curr_cost = fast_cost
    while curr_cost > max_cost:
        # find best savings
        best_saving = None
        for i in range(nr_points):
            for ec2host, times in timeline.items():
                opp_cost = (lookup_cost(loc[i], cost_map) - lookup_cost(ec2host, cost_map)) * step_h
                if opp_cost > 0:
                    opp_resp = timeline[loc[i]][i][1] - times[i][1]
                    opp_total = opp_cost / opp_resp
                    if best_saving is None or opp_total > best_saving[2]:
                        best_saving = (i, ec2host, opp_total, opp_cost)
        # apply it
        if best_saving is None:
            print('min_cost=%.3f reached' % curr_cost)
            break
        curr_cost -= best_saving[3]
        loc[best_saving[0]] = best_saving[1]
        print('curr_cost=%.3f' % curr_cost)
    return loc


#---------------------------------------------------------------------------------------------------------------------
# Plot helpers
#---------------------------------------------------------------------------------------------------------------------

class Res:
    @staticmethod
    def init():
        Res.scale = 2.0
        # http://xoomer.virgilio.it/infinity77/wxPython/Widgets/wx.ColourDatabase.html
        Res.colors = ['RED', 'BLUE', 'GREY', 'GREEN', 'YELLOW', 'BROWN', 'AQUAMARINE', 'PURPLE']
        Res.font_title = wx.Font(16 * Res.scale, wx.FONTFAMILY_ROMAN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD, False, 'DejaVu Serif')
        #Res.font_title = wx.Font(16 * Res.scale, wx.FONTFAMILY_ROMAN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD, False, 'Helvetica')
        Res.font_description = wx.Font(11 * Res.scale, wx.FONTFAMILY_ROMAN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD, False, 'DejaVu Serif')
        #Res.font_description = wx.Font(11 * Res.scale, wx.FONTFAMILY_ROMAN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD, False, 'Helvetica')
        Res.font_names = wx.Font(10 * Res.scale, wx.FONTFAMILY_SWISS, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL, False, 'Droid Sans')
        #Res.font_names = wx.Font(10 * Res.scale, wx.FONTFAMILY_ROMAN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL, False, 'Tahoma')
        Res.font_labels = wx.Font(8 * Res.scale, wx.FONTFAMILY_SWISS, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL, False, 'Liberation Sans Narrow')
        #Res.font_labels = wx.Font(8 * Res.scale, wx.FONTFAMILY_TELETYPE, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_NORMAL, False, 'BaseMono') # Lucida Console, Andale Mono
        Res.dash_pen = wx.Pen('#DDDDDD', 1, wx.USER_DASH)
        Res.dash_pen.SetDashes([4, 4])
        Res.dash_pen.SetCap(wx.CAP_ROUND)
        Res.axes_pen = wx.Pen('BLACK', 1 + 2 * Res.scale, wx.SOLID)
        Res.axes_pen.SetCap(wx.CAP_BUTT)
        Res.grid_pen = wx.Pen('#EEEEEE', Res.scale, wx.SOLID)
        Res.grid_pen.SetCap(wx.CAP_BUTT)
        Res.line_pen = wx.Pen('BLACK', 1 + Res.scale, wx.SOLID)
        Res.line_pen.SetCap(wx.CAP_ROUND)
        Res.legend_pen = wx.Pen('BLACK', 6 * Res.scale, wx.SOLID)
        Res.legend_pen.SetCap(wx.CAP_BUTT)
        Res.circle_pen = wx.Pen('BLACK', 1 * Res.scale, wx.SOLID)
        Res.no_brush = wx.Brush('WHITE', wx.TRANSPARENT)
        Res.no_pen = wx.Pen('WHITE', 1, wx.TRANSPARENT)



def add_transparency(bmp, color):
    print('\n--- Adding transparency..')
    timg = bmp.ConvertToImage()
    if not timg.HasAlpha():
        timg.InitAlpha()
    for y in xrange(timg.GetHeight()):
        for x in xrange(timg.GetWidth()):
            pix = wx.Colour(timg.GetRed(x, y), timg.GetGreen(x, y), timg.GetBlue(x, y))
            if pix == Color:
                timg.SetAlpha(x, y, 0)
    bmp = timg.ConvertToBitmap()
    return bmp


def create_image(width, height):
    bmp = wx.EmptyBitmap(width, height)
    #bmp = wx.EmptyBitmapRGBA(width, height, 200, 100, 100, 0)
    #bmp.UseAlpha()
    dc = wx.MemoryDC()
    dc.SelectObject(bmp)
    #dc.SetBackground(wx.Brush('WHITE', wx.TRANSPARENT))
    dc.SetBackground(wx.Brush('WHITE', wx.SOLID))
    dc.SetBackgroundMode(wx.TRANSPARENT)
    dc.Clear()
    return bmp, dc


def save_image(bmp, dc, plot_desc, transparent=False):
    dc.SelectObject(wx.NullBitmap)
    if transparent:
        bmp = add_transparency(bmp, 'WHITE')
    filename = os.path.join(out_dir, plot_desc['filename'] + '.png')
    print('\n--- Saving %s..' % filename)
    bmp.SaveFile(filename, wx.BITMAP_TYPE_PNG)


def add_header(dc, plot_desc, img_w, header_h):
    dc.SetTextForeground('BLACK')
    dc.SetFont(Res.font_title)
    tw, th = dc.GetTextExtent(plot_desc['title'])
    dc.DrawText(plot_desc['title'], (img_w - tw) / 2,  (header_h/2 - th) / 2)
    dc.SetTextForeground('#777777')
    dc.SetFont(Res.font_description)
    tw, th = dc.GetTextExtent(plot_desc['description'])
    dc.DrawText(plot_desc['description'], (img_w - tw) / 2,  header_h/2 + (header_h/4 - th) / 2)


def add_axis(dc, zero_x, zero_y, len_x, len_y):
    dc.SetPen(Res.axes_pen)
    dc.DrawLine(zero_x, zero_y, zero_x, zero_y - len_y)
    dc.DrawLine(zero_x, zero_y, zero_x + len_x, zero_y)


def add_x_label(dc, pos_x, pos_y, text='', angle=-45):
    if text is None or len(text) <= 0:
        return
    marker_h = 5 * Res.scale
    gap_h = 2 * Res.scale
    dc.SetPen(Res.axes_pen)
    dc.DrawLine(pos_x, pos_y, pos_x, pos_y + marker_h)
    dc.SetTextForeground('#777777')
    dc.SetFont(Res.font_labels)
    tw, th = dc.GetTextExtent(text)
    if angle is None or angle == 0:
        dc.DrawText(text, pos_x - tw/2, pos_y + marker_h + gap_h)
    else:
        dc.DrawRotatedText(text, pos_x, pos_y + marker_h + gap_h, angle)


def add_y_label(dc, pos_x, pos_y, text=''):
    if text is None or len(text) <= 0:
        return
    marker_w = 5 * Res.scale
    gap_w = 2 * Res.scale
    dc.SetPen(Res.axes_pen)
    dc.DrawLine(pos_x, pos_y, pos_x - marker_w, pos_y)
    dc.SetTextForeground('#777777')
    dc.SetFont(Res.font_labels)
    tw, th = dc.GetTextExtent(text)
    dc.DrawText(text, pos_x - marker_w - gap_w - tw, pos_y - th/2)


def add_titles(dc, plot_desc, center_0x_x, center_0x_y, center_0y_x, center_0y_y):
    dc.SetTextForeground('#444444')
    dc.SetFont(Res.font_names)
    tw, th = dc.GetTextExtent(plot_desc['h_name'])
    dc.DrawText(plot_desc['h_name'], center_0x_x - tw/2, center_0x_y - th/2)
    tw, th = dc.GetTextExtent(plot_desc['v_name'])
    dc.DrawRotatedText(plot_desc['v_name'], center_0y_x - th/2, center_0y_y + tw/2, 90)


def add_grid(dc, xs, ys, zero_x, zero_y, len_x, len_y):
    dc.SetPen(Res.grid_pen)
    for x in xs:
        dc.DrawLine(x, zero_y, x, zero_y - len_y)
    for y in ys:
        dc.DrawLine(zero_x, y, zero_x + len_x, y)


def add_legend(dc, labels, colors, img_w, legend_h, pos_y):
    assert (len(labels) <= len(colors))
    dc.SetTextForeground('#777777')
    dc.SetFont(Res.font_labels)
    key_w = 25 * Res.scale
    keygap_w = 3 * Res.scale
    space_w = 25 * Res.scale
    sum_w, sum_h = group_dimensions(dc, labels, Res.font_labels, sum)
    total_w = sum_w + len(labels) * (key_w + keygap_w + space_w) - space_w
    curr_x = (img_w - total_w)/2
    for i, label in enumerate(labels):
        Res.legend_pen.SetColour(colors[i])
        dc.SetPen(Res.legend_pen)
        tw, th = dc.GetTextExtent(label)
        dc.DrawText(label, curr_x, pos_y + (legend_h - th)/2)
        curr_x += tw + keygap_w
        dc.DrawLine(curr_x, pos_y + legend_h/2, curr_x + key_w, pos_y + legend_h/2)
        curr_x += key_w + space_w


def add_blimp(dc, pos_x, pos_y, radius):
    dc.SetPen(Res.circle_pen)
    dc.SetBrush(Res.no_brush)
    dc.DrawCircle(pos_x, pos_y, radius + 3 * Res.scale)
    dc.DrawCircle(pos_x, pos_y, radius)
    dc.DrawCircle(pos_x, pos_y, radius - 3 * Res.scale)


def get_short_ec2hosts(ec2hosts):
    return [host[host.find('.')+1:] for host in ec2hosts]

def lookup_cost(ec2host, cost_map):
    return cost_map[ec2host[ec2host.find('.')+1:]]

def sorted_ec2hosts(ec2hosts):
    return sorted(ec2hosts, key=lambda x: x[x.find('.')+1:])


def sorted_plhosts(plhosts):
    return sorted(plhosts, key=lambda x: plhosts_info[x]['longitude'])


def group_dimensions(dc, labels, font, func=max):
    prev_font = dc.GetFont()
    dc.SetFont(font)
    dims = [dc.GetTextExtent(label) for label in labels]
    group_w = func(map(lambda e: e[0], dims))
    group_h = func(map(lambda e: e[1], dims))
    dc.SetFont(prev_font)
    return group_w, group_h


def rescale(val, val_min, val_max, range_min, range_max):
    assert val_min < val_max
    assert range_min != range_max
    return range_min + (val - val_min) * (range_max - range_min) / (val_max - val_min)


def get_significant_times(start_timestamp, end_timestamp):
    assert start_timestamp < end_timestamp
    days = float(end_timestamp - start_timestamp) / (3600 * 24)
    main_pt = list()
    init_time = time.localtime(start_timestamp)
    init_timestamp = start_timestamp - 3600 * init_time.tm_hour - 60 * init_time.tm_min
    if days <= 1: # main every 6h, sec every 3h
        main_delta = 6 * 3600
        use_weekday = False
        use_date = False
        use_hour = True
    elif days <= 3: # main every 12h, sec every 6h
        main_delta = 12 * 3600
        use_weekday = True
        use_date = True
        use_hour = True
    else: # main every 24h, sec every 12h
        main_delta = 24 * 3600
        use_weekday = True
        use_date = init_time.tm_year > 2000
        use_hour = False
    timestamp = init_timestamp
    while timestamp <= end_timestamp:
        if timestamp >= start_timestamp:
            main_pt.append((time_str(timestamp, use_weekday, use_date, use_hour), timestamp))
            timestamp_sec = timestamp + main_delta/2
            if timestamp_sec <= end_timestamp:
                main_pt.append((None, timestamp_sec))
        timestamp += main_delta
    return main_pt


def get_significant_values(val_min, val_max, count=4):
    assert val_min < val_max
    main_pt = list()
    step = float(val_max - val_min) / count
    val = val_min
    while val <= val_max:
        main_pt.append(('%.3f' % val, val))
        val_sec = val + step/2
        if val_sec <= val_max:
            main_pt.append((None, val_sec))
        val += step
    if main_pt[-1][1] != val_max:
        main_pt.append(('%.3f' % val_max, val_max))
    return main_pt


#---------------------------------------------------------------------------------------------------------------------
# Plot methods
#---------------------------------------------------------------------------------------------------------------------

def do_plot_timeline(records, plot_desc):
    records = remap_time(records, plot_desc['params']['timeremap'])
    timeline, start_time, end_time, val_min, val_max = get_ranking_timeline(records, plot_desc)

    print('\n--- Plotting timeline..')

    img_h = 600 * Res.scale
    header_h = 75 * Res.scale
    legend_h = 50 * Res.scale
    footer_h = 20 * Res.scale
    lrpad_w = 40 * Res.scale
    label_x_h = 70 * Res.scale
    label_y_w = 40 * Res.scale
    title_x_h = 20 * Res.scale
    title_y_w = 20 * Res.scale
    plot_pad = 20 * Res.scale

    ptspacing_w = plot_desc['params']['dotpitch'] * Res.scale

    if plot_desc['params']['zeromin']:
        val_min = 0

    nr_points = len(timeline[timeline.keys()[0]]) # assume all lengths equal
    hosts = sorted_ec2hosts(timeline.keys())

    plot_w = (nr_points-1) * ptspacing_w
    plot_h = img_h - header_h - label_x_h - title_x_h - legend_h - footer_h

    zero_x = lrpad_w + title_y_w + label_y_w
    zero_y = header_h + plot_h

    img_w = 2 * lrpad_w + title_y_w + label_y_w + plot_w

    bmp, dc = create_image(img_w, img_h)

    add_header(dc, plot_desc, img_w, header_h)
    add_titles(dc, plot_desc,
        zero_x + plot_w/2, img_h - footer_h - legend_h - title_x_h/2,
        lrpad_w + title_y_w/2, zero_y - plot_h/2)

    curr_row = 0
    row_count = 2 if img_w < 3 * img_h else 1
    col_count = len(hosts)//row_count + len(hosts)%row_count%2
    while curr_row < row_count:
        add_legend(dc,
            get_short_ec2hosts(hosts[curr_row*col_count:(curr_row+1)*col_count]),
            Res.colors[curr_row*col_count:(curr_row+1)*col_count],
            img_w, legend_h/row_count, img_h - footer_h - legend_h + curr_row * legend_h/row_count)
        curr_row += 1

    #add_legend(dc, hosts, Res.colors, img_w, legend_h, img_h - footer_h - legend_h)

    grid_xs = list()
    time_pts = get_significant_times(start_time, end_time)
    for time_pt in time_pts:
        pos_x = rescale(time_pt[1], start_time, end_time, zero_x, zero_x + plot_w)
        grid_xs.append(pos_x)
        add_x_label(dc, pos_x, zero_y, time_pt[0])

    grid_ys = list()
    plot_y_max = zero_y - plot_h
    plot_y_min = zero_y - (plot_pad if val_min != 0 else 0)
    val_pts = get_significant_values(val_min, val_max)
    for val_pt in val_pts:
        pos_y = rescale(val_pt[1], val_min, val_max, plot_y_min, plot_y_max)
        grid_ys.append(pos_y)
        add_y_label(dc, zero_x, pos_y, val_pt[0])

    add_grid(dc, grid_xs, grid_ys, zero_x, zero_y, plot_w, plot_h)

    for i, lbl in enumerate(hosts):
        print('Plotting %s..' % lbl)
        records = timeline[lbl]
        Res.line_pen.SetColour(Res.colors[i])
        dc.SetPen(Res.line_pen)
        last_x = zero_x - ptspacing_w
        last_y = None
        for record in records:
            curr_x = last_x + ptspacing_w
            if record[1] is not None:
                curr_y = rescale(record[1], val_min, val_max, plot_y_min, plot_y_max)
                if last_y is not None:
                    dc.DrawLine(last_x, last_y, curr_x, curr_y)
            else:
                curr_y = None
            last_x, last_y = curr_x, curr_y

    if 'costs' in plot_desc['params']:
        marker_size = 5 * Res.scale
        bar_h = 5 * Res.scale
        print('Computing location plan..')
        loc = get_location_plan(timeline, plot_desc)
        print('Plotting location plan..')
        for i in range(nr_points):
            pos_x = zero_x + ptspacing_w * i
            pos_y = rescale(timeline[loc[i]][i][1], val_min, val_max, plot_y_min, plot_y_max)
            add_blimp(dc, pos_x, pos_y, marker_size)
            color = Res.colors[hosts.index(loc[i])]
            dc.SetBrush(wx.Brush(color, wx.SOLID))
            dc.SetPen(Res.no_pen)
            dc.DrawRectangle(pos_x - ptspacing_w/2, img_h - title_x_h - legend_h - footer_h - 4 * bar_h, ptspacing_w, bar_h)

    add_axis(dc, zero_x, zero_y, plot_w, plot_h)
    save_image(bmp, dc, plot_desc)


def do_plot_cdf(records, plot_desc):
    cdf_data, val_min, val_max = get_cdf(records, plot_desc)

    print('\n--- Plotting CDF..')

    img_h = 800 * Res.scale
    img_w = 750 * Res.scale
    header_h = 75 * Res.scale
    legend_h = 75 * Res.scale
    footer_h = 20 * Res.scale
    lrpad_w = 40 * Res.scale
    label_x_h = 30 * Res.scale
    label_y_w = 40 * Res.scale
    title_x_h = 30 * Res.scale
    title_y_w = 20 * Res.scale
    plot_pad = 20 * Res.scale

    hosts = sorted_ec2hosts(cdf_data.keys())

    plot_w = img_w - lrpad_w*2 - label_y_w - title_y_w
    plot_h = img_h - header_h - label_x_h - title_x_h - legend_h - footer_h

    zero_x = lrpad_w + title_y_w + label_y_w
    zero_y = header_h + plot_h

    bmp, dc = create_image(img_w, img_h)

    add_header(dc, plot_desc, img_w, header_h)
    add_titles(dc, plot_desc,
        zero_x + plot_w/2, img_h - footer_h - legend_h - title_x_h/2,
        lrpad_w + title_y_w/2, zero_y - plot_h/2)

    curr_row = 0
    row_count = len(hosts)//2 + len(hosts)%2
    col_count = 2
    while curr_row < row_count:
        add_legend(dc,
            get_short_ec2hosts(hosts[curr_row*col_count:(curr_row+1)*col_count]),
            Res.colors[curr_row*col_count:(curr_row+1)*col_count],
            img_w, legend_h/row_count, img_h - footer_h - legend_h + curr_row * legend_h/row_count)
        curr_row += 1

    grid_xs = list()
    plot_x_max = zero_x + plot_w
    plot_x_min = zero_x + (plot_pad if val_min != 0 else 0)
    val_pts = get_significant_values(val_min, val_max)
    for val_pt in val_pts:
        pos_x = rescale(val_pt[1], val_min, val_max, plot_x_min, plot_x_max)
        grid_xs.append(pos_x)
        add_x_label(dc, pos_x, zero_y, val_pt[0])

    grid_ys = list()
    val_pts = get_significant_values(0, 1, 5)
    for val_pt in val_pts:
        pos_y = rescale(val_pt[1], 0, 1, zero_y, zero_y - plot_h)
        grid_ys.append(pos_y)
        add_y_label(dc, zero_x, pos_y, val_pt[0])

    add_grid(dc, grid_xs, grid_ys, zero_x, zero_y, plot_w, plot_h)

    plot_x_step = float(plot_x_max - plot_x_min) / plot_desc['params']['steps']

    for i, lbl in enumerate(hosts):
        print('Plotting %s..' % lbl)
        values = cdf_data[lbl]
        Res.line_pen.SetColour(Res.colors[i])
        dc.SetPen(Res.line_pen)
        last_x = plot_x_min - plot_x_step
        last_y = None
        for perc in values:
            curr_x = last_x + plot_x_step
            curr_y = rescale(perc, 0, 1, zero_y, zero_y - plot_h)
            if last_y is not None:
                dc.DrawLine(last_x, last_y, curr_x, curr_y)
            last_x, last_y = curr_x, curr_y

    add_axis(dc, zero_x, zero_y, plot_w, plot_h)
    save_image(bmp, dc, plot_desc)


#---------------------------------------------------------------------------------------------------------------------
# Misc
#---------------------------------------------------------------------------------------------------------------------

def dump_csv(records, filename, request):
    with open(filename, 'w') as f:
        for ec2host, time_records in records.items():
            for time_record in time_records:
                time_struct = time.gmtime(time_record[0] + 6 * 3600)
                for timing in time_record[1]:
                    f.write('%s, %s, %s, %s, %d, %d, %s, %s, %.4f, %d, %d, %d, %.3f, %.3f\n' % (
                        ec2host,
                        request,
                        timing[0],
                        time.strftime('%d.%m.%Y %H:%M', time_struct),
                        time_struct.tm_mday,
                        time_struct.tm_mon,
                        time.strftime('%H:%M', time_struct),
                        time.strftime('%a', time_struct),
                        timing[1],
                        time_struct.tm_hour,
                        time_struct.tm_min,
                        0,
                        plhosts_info[timing[0]]['latitude'],
                        plhosts_info[timing[0]]['longitude']
                    ))


#---------------------------------------------------------------------------------------------------------------------
# Main
#---------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    app = wx.App()
    app.MainLoop()
    Res.init()
    ensure_path(out_dir)
    if not os.path.exists(plhosts_file):
        store_plhosts_info(get_plhosts_info(), plhosts_file)
    if not os.path.exists(pickle_blob):
        store_data(collect_data(data_dirs), pickle_blob)
    plhosts_info = load_plhosts_info(plhosts_file)
    all_data = load_data(pickle_blob)
    for plot_desc in plots:
        print('\n\n=== Processing plot %s..' % plot_desc['filename'])
        records, plhosts = filter_data(all_data, plot_desc)
        #print(compute_plhosts_stats(plhosts))
        plot(records, plhosts, plot_desc)
    for plot_desc in csv_dump:
        print('\n\n=== Processing dataset %s..' % plot_desc['filename'])
        records, plhosts = filter_data(all_data, plot_desc)
        dump_csv(records, 'dump-%s.csv' % plot_desc['filename'], plot_desc['filename'])
    print('\n=== All done!\n')


# {'Europe': 39, 'North America': 42, 'South America': 5, 'Asia': 18}





