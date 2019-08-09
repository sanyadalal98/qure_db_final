from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2
from rest_framework import status
from rest_framework.response import Response
import calendar
import logging
from .clients import client_details
import requests
import re
from rest_framework.decorators import api_view
logger = logging.getLogger(__name__)


def connect_cxr_database():
    conn = psycopg2.connect(
        "dbname='' user='' host='' port='' password=''")
    logger.info("Connection to CXR Database Done.")
    return conn

def connect_cxr_eu_database():
    conn = psycopg2.connect(
        "dbname='' user='' host='' port='' password=''")
    logger.info("Connection to CXR EU Database Done")
    return conn

def connect_hct_beta_database():
    conn = psycopg2.connect(
        "dbname='' user='' host='' port='' password=''")
    logger.info("Connection to HCT Beta Database Done.")
    return conn

def connect_hct_eu_database():
    conn = psycopg2.connect(
        "dbname='' user='' host='' port='' password=''")
    logger.info("Connection to HCT Beta Database Done.")
    return conn

def connect_onpremise_csv():
    response = requests.get("", headers={'Authorization': '', 'Dropbox-API-Arg': '{"path": ""}'})
    logger.info("Connection to Onpremise Dropbox file made")
    return response.text

def calc_onpremise_details():
    data = connect_onpremise_csv()
    qxr_add = 0
    qer_add = 0
    total_sites = 0
    #skip first line because table headers
    i = 1
    lines = re.split("[\r\n]+", data)
    while i < len(lines):
        points = [x.strip() for x in lines[i].split(',')]
        #chech if line isn't empty
        if any(item != "" for item in points):
            #to get total number of sites
            if points[0] == "TOTAL NUMBER OF SITES":
                total_sites += int(points[3])
            #for actual onpremise clients
            else:
                if (points[2] == "qXR") or (points[2] == "qXR-qCheck"):
                    qxr_add += int(points[3])
                elif (points[2] == "qER") or (points[2] == "qER-qCheck"):
                    qer_add += int(points[3])
        i += 1
    return (str(qxr_add) + " " + str(qer_add) + " " + str(total_sites))

def get_qxr_details(cur, qxr_obj, db_name):
    access_tb_condition = date(2018, 8, 5)
    week_scans = 0
    for row in cur:
        if row[1] == "accesstb" and db_name == "us" and row[0] >= access_tb_condition:
            continue
        if row[1] == "accesstb" and db_name == "eu" and row[0] < access_tb_condition:
            continue
        qxr_obj["qxr_scans"] += int(row[2])
        qxr_obj["tb_positive"] += int(row[3])
        this_date = datetime.combine(row[0], datetime.min.time())
        #check if scan is within the last month (then can use it for runtime calculation)
        if datetime.today() - relativedelta(months=1) <= this_date:
            #check if runtime < 5000s
            if int(row[4]) < 5000:
                qxr_obj["qxr_count"] += int(row[2])
                qxr_obj["qxr_runtime"] += (int(row[2]) * int(row[4]))
        #check if scan is within the last week (then can use it for scans per week calculations)
        if datetime.today() - relativedelta(days=7) <= this_date:
            week_scans += int(row[2])
    return week_scans

def get_qer_details(cur, qer_obj):
    week_scans = 0
    for row in cur:
        qer_obj["qer_scans"] += int(row[1])
        #check if runtime > 0
        if int(row[2]) > 0:
            qer_obj["qer_count"] += int(row[1])
            qer_obj["qer_runtime"] += (int(row[1]) * int(row[2]))
        #check if scan was sent within the last week
        this_date = datetime.combine(row[0], datetime.min.time())
        if datetime.today() - relativedelta(days=7) <= this_date:
            week_scans += int(row[1])
    return week_scans

@api_view(['GET'])
def get_details(request):
    qxr_details = {"qxr_scans": 22625, "qxr_runtime": 0, "tb_positive": 0, "qxr_count": 0}
    qer_details = {"qer_scans": 561, "qer_runtime": 0, "qer_count": 0}
    this_week_scans = 0
    #get data from cxr oregon database
    cur1 = None
    try:
        conn = connect_cxr_database()
        logger.info("I am able to connect to the qxr oregon database.....\n")
    except Exception as e:
        logger.error(str(e))
        logger.error('error occured while connecting to qxr oregon database')
    try:
        cur1 = conn.cursor()
        logger.info("Connection done, execution in progress")
        cur1.execute(
            """SELECT received_on::date, source, neg_cases + pos_cases as total, pos_cases as tb_positive, avg_time FROM get_db_daily_stats() where tag='tuberculosis'""")
        logger.info("qXR Calculation is in Progress.....\n")
        this_week_scans += get_qxr_details(cur1, qxr_details, "us")
    except Exception as e:
        logger.error(str(e))
        logger.error('error occured while performing qxr calculations for qxr oregon database')
    finally:
        if cur1:
            cur1.close()
    #get data from hct oregon database
    cur1 = None
    try:
        conn = connect_hct_beta_database()
        logger.info("I am able to connect to the qer oregon database.....\n")
    except:
        logger.error('error occured while connecting to qer database')
    try:
        cur1 = conn.cursor()
        cur1.execute(
            """SELECT received_on::date, total, avg_time FROM get_db_daily_stats()""")
        logger.info("qER Calculation is in Progress.....\n")
        this_week_scans += get_qer_details(cur1, qer_details)
    except:
        logger.error('error occured while performing qer calculations')
    finally:
        if cur1:
            cur1.close()
    #get data from cxr eu database
    cur1 = None
    try:
        conn = connect_cxr_eu_database()
        logger.info("I am able to connect to the qxr eu database.....\n")
    except Exception as e:
        logger.error(str(e))
        logger.error('error occured while connecting to qxr eu database')
    try:
        cur1 = conn.cursor()
        logger.info("Connection done, execution in progress")
        cur1.execute(
            """SELECT received_on::date, source, neg_cases + pos_cases as total, pos_cases as tb_positive, avg_time FROM get_db_daily_stats() where tag='tuberculosis'""")
        logger.info("qXR Calculation is in Progress.....\n")
        this_week_scans += get_qxr_details(cur1, qxr_details, "eu")
    except Exception as e:
        logger.error(str(e))
        logger.error('error occured while performing qxr calculations')
    finally:
        if cur1:
            cur1.close()
    #get data from hct eu database
    # cur1 = None
    # try:
    #     conn = connect_hct_eu_database()
    #     logger.info("I am able to connect to the qer eu database.....\n")
    # except:
    #     logger.error('error occured while connecting to qer eu database')
    # try:
    #     cur1 = conn.cursor()
    #     cur1.execute(
    #         """SELECT received_on::date, total, avg_time FROM get_db_daily_stats()""")
    #     logger.info("qER Calculation is in Progress.....\n")
    #     this_week_scans += get_qer_details(cur1, qer_details)
    # except:
    #     logger.error('error occured while performing qer calculations')
    # finally:
    #     if cur1:
    #         cur1.close()
    #perform final calculations
    qxr_details["qxr_runtime"] = int(qxr_details["qxr_runtime"]/qxr_details["qxr_count"])
    qer_details["qer_runtime"] = int(qer_details["qer_runtime"]/qer_details["qer_count"])
    this_week_scans = int(this_week_scans/7)
    onpremise_additions = calc_onpremise_details().split()
    qxr_details['qxr_scans'] = qxr_details['qxr_scans'] + int(onpremise_additions[0])
    qer_details['qer_scans'] = qer_details['qer_scans'] + int(onpremise_additions[1])
    lives = qxr_details['qxr_scans'] + qer_details['qer_scans']
    response = {"lives_impacted": '{:,}'.format(lives), "qxr_scans": '{:,}'.format(qxr_details['qxr_scans']), "qer_scans": '{:,}'.format(qer_details['qer_scans']), "qxr_runtime": str(qxr_details['qxr_runtime']), "qer_runtime": str(qer_details['qer_runtime']), "avg_scans": '{:,}'.format(this_week_scans), "tb_positive": '{:,}'.format(qxr_details['tb_positive']), "num_site": onpremise_additions[2]}
    return JsonResponse(response)

self_testing = ["devqure", "testing", "scanportal", "Source object"]
miscellaneous = ["stardigital", "stardigital.path" ,"samarpan", "shrisai.path", "parate", "abhinav_ngp","ramdev", "path_unmapped"]

def add_to_dictionary(cur, l, qType):
    #these qxr clients already exist under another name - to avoid duplicates, these are ignored:
    qxr_ignore_cases = ["accesstb_unmapped","fivecqure", "incepto_testing", "incepto_eu", "mv_brazil", "fhi360", "ge"]
    #accesstb qer data was a test bu qure, so ignore it
    qer_ignore_cases = ["accesstb"]
    for row in cur:
        client = row[1]
        if client in self_testing:
            continue
        if qType == "qXR" and client in qxr_ignore_cases:
            continue
        if qType == "qER" and client in qer_ignore_cases:
            continue
        if client in miscellaneous:
            client = "Miscellaneous"
        scan_date = row[0]
        this_day = datetime.today()
        newdate = None
        if scan_date.month == this_day.month and scan_date.year == this_day.year:
            newdate = this_day
        else:
            last_day = calendar.monthrange(scan_date.year, scan_date.month)
            newdate = scan_date.replace(day = last_day[1])
        curr_date = newdate.strftime("%Y-%m-%d")
        for obj in l:
            if obj["t"] == curr_date:
                if not client in obj["y"]:
                    obj["y"].append(client)

def add_clients_cumu(l):
        i = 0
        while i < (len(l) - 1):
            for client in l[i]["y"]:
                if not client in l[i+1]["y"]:
                    l[i+1]["y"].append(client)
            i += 1

def count_clients(l):
    for client in l:
        number = len(client["y"])
        client["y"] = str(number)

def get_chart_points(request):
    response = dict()
    qxr_clients = []
    qer_clients = []
    #fill clients_per_month with objects with date & clients = []
    start_date = '2018-6-20'
    end_date = date.today()
    cur_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    while cur_date < end_date:
        last_day = calendar.monthrange(cur_date.year, cur_date.month)
        newdate = cur_date.replace(day = last_day[1])
        if newdate.month == end_date.month and newdate.year == end_date.year:
            newdate = end_date
        qxr_clients.append({"t": newdate.strftime("%Y-%m-%d"), "y": []})
        qer_clients.append({"t": newdate.strftime("%Y-%m-%d"), "y": []})
        cur_date += relativedelta(months=1)
    qxr_clients.append({"t": datetime.today().strftime("%Y-%m-%d"), "y":[]})
    qer_clients.append({"t": datetime.today().strftime("%Y-%m-%d"), "y":[]})
    #add clients from qxr oregon
    cur1 = None
    try:
        conn = connect_cxr_database()
        logger.info("I am able to connect to the database.....\n")
    except:
        logger.error('error occured while connecting to qxr oregon database')
    try:
        cur1 = conn.cursor()
        logger.info("Connection Done, Execution in Progress.....\n")
        cur1.execute(
            """SELECT received_on::date, source FROM get_db_daily_stats() where tag='tuberculosis'""")
        logger.info("Execution Done, Adding to dictionary.....\n")
        add_to_dictionary(cur1, qxr_clients, "qXR")
    except:
        logger.exception('error occured when adding qxr oregon clients to dicitonary')
    finally:
        if cur1:
            cur1.close()
    #add clients from qxr eu
    cur1 = None
    try:
        conn = connect_cxr_eu_database()
        logger.info("I am able to connect to the qxr eu database.....\n")
    except:
        logger.error('error occured while connecting to eu database')
    try:
        cur1 = conn.cursor()
        logger.info("Connection Done, Execution in Progress.....\n")
        cur1.execute(
            """SELECT received_on::date, source FROM get_db_daily_stats() where tag='tuberculosis'""")
        logger.info("Execution Done, Adding to dictionary.....\n")
        add_to_dictionary(cur1, qxr_clients, "qXR")
    except:
        logger.exception('error occured when adding qxr eu clients to dicitonary')
    finally:
        if cur1:
            cur1.close()
    #add clients from qer oregon
    cur2 = None
    try:
        conn = connect_hct_beta_database()
        logger.info("I am able to connect to the database.....\n")
    except:
        logger.error('error occured while connecting to database')
    try:
        cur2 = conn.cursor()
        logger.info("Connection Done, Execution in Progress.....\n")
        cur2.execute(
            """SELECT received_on::date, source FROM get_db_daily_stats()""")
        logger.info("Execution Done, Adding to dictionary.....\n")
        add_to_dictionary(cur2, qer_clients, "qER")
    except:
        logger.exception('error occured when adding qer clients to dicitonary')
    finally:
        if cur2:
            cur2.close()
    #add clients cumulatively
    add_clients_cumu(qxr_clients)
    add_clients_cumu(qer_clients)
    #count clients
    count_clients(qxr_clients)
    count_clients(qer_clients)
    #find max number of clients (qer or qxr)
    qxr_max = max(int(entry["y"]) for entry in qxr_clients)
    qer_max = max(int(entry["y"]) for entry in qer_clients)
    graph_upper_limit = max(qxr_max, qer_max) + 5
    response['max'] = str(graph_upper_limit)
    response['qxr_points'] = qxr_clients
    response['qer_points'] = qer_clients
    return JsonResponse(response)

def merge_clients(l, repeats):
    for set in repeats:
        origin = set[0]
        i = 1
        while i < len(set):
            cur_name = set[i]
            if datetime.strptime(l[origin]["start"], "%Y-%m-%d") > datetime.strptime(l[cur_name]["start"], "%Y-%m-%d"):
                l[origin]["start"] = l[cur_name]["start"]
            if datetime.strptime(l[origin]["last_date"], "%Y-%m-%d") < datetime.strptime(l[cur_name]["last_date"], "%Y-%m-%d"):
                l[origin]["last_date"] = l[cur_name]["last_date"]
            l[origin]["total"] = int(l[origin]["total"]) + int(l[cur_name]["total"])
            d = datetime.now() - datetime.strptime(l[origin]["start"], "%Y-%m-%d")
            avg = int(l[origin]["total"])/d.days
            l[origin]["avg_scans"] = str(int(avg))
            del l[cur_name]
            i += 1

def fill_table(cur, l, qType, db_name):
    qer_ignore_cases = ["accesstb"]
    access_tb_condition = date(2018, 8, 5)
    for row in cur:
        name = row[1]
        if name in self_testing:
            continue
        if qType == "qER" and name in qer_ignore_cases:
            continue
        if name == "accesstb" and db_name == "qxr_us" and row[0] >= access_tb_condition:
            continue
        if name == "accesstb" and db_name == "qxr_eu" and row[0] < access_tb_condition:
            continue
        if name in miscellaneous:
            name = "Others: PATH"
        ti = row[0]
        day = datetime(ti.year, ti.month, ti.day)
        uploads = int(row[2])
        if name in l:
            if datetime.strptime(l[name]["start"], "%Y-%m-%d") > day:
                l[name]["start"] = day.strftime("%Y-%m-%d")
            if datetime.strptime(l[name]["last_date"], "%Y-%m-%d") < day:
                l[name]["last_date"] = day.strftime("%Y-%m-%d")
            l[name]["total"] = str(int(l[name]["total"]) + uploads)
        else:
            l[name] = {}
            if name in client_details and client_details[name]["fullName"]:
                    l[name]["full_name"] = client_details[name]["fullName"]
            else:
                l[name]["full_name"] = name
            if name in client_details and client_details[name]["country"]:
                    l[name]["location"] = client_details[name]["country"]
            else:
                l[name]["location"] = ""
            if qType == "qXR":
                l[name]["qXR"] = "true"
                l[name]["qER"] = "false"
            else:
                l[name]["qXR"] = "false"
                l[name]["qER"] = "true"
            l[name]["start"] = day.strftime("%Y-%m-%d")
            l[name]["total"] = str(uploads)
            l[name]["last_date"] = day.strftime("%Y-%m-%d")
        delta = datetime.now() - datetime.strptime(l[name]["start"], "%Y-%m-%d")
        d = delta.days
        if d < 1 :
            d = 1
        average = (int(l[name]["total"])/d)
        l[name]["avg_scans"] = str(int(average))

def get_table_data(request):
    entries_qxr = dict()
    entries_qer = dict()
    #add qXR clients from oregon database
    cur1 = None
    try:
        conn = connect_cxr_database()
        logger.info("I am able to connect to the database.....\n")
    except:
        logger.error('error occured while connecting to qxr oregon database')
    try:
        cur1 = conn.cursor()
        logger.info("Connection Done, Execution in Progress.....\n")
        cur1.execute(
            """SELECT received_on::date, source, neg_cases + pos_cases as total FROM get_db_daily_stats() where tag='tuberculosis'""")
        logger.info("Execution Done, Adding to table.....\n")
        fill_table(cur1, entries_qxr, "qXR", "qxr_us")
    except:
        logger.exception('error occured when adding qxr oregon clients to table')
    finally:
        if cur1:
            cur1.close()
    #add qxr clients from eu database
    cur1 = None
    try:
        conn = connect_cxr_eu_database()
        logger.info("I am able to connect to the database.....\n")
    except:
        logger.error('error occured while connecting to qxr eu database')
    try:
        cur1 = conn.cursor()
        logger.info("Connection Done, Execution in Progress.....\n")
        cur1.execute(
            """SELECT received_on::date, source, neg_cases + pos_cases as total FROM get_db_daily_stats() where tag='tuberculosis'""")
        logger.info("Execution Done, Adding to table.....\n")
        fill_table(cur1, entries_qxr, "qXR", "qxr_eu")
    except:
        logger.exception('error occured when adding qxr eu clients to table')
    finally:
        if cur1:
            cur1.close()
    #merge repeated clients
    qxr_repeated_clients = [["accesstb", "accesstb_unmapped"], ["fivec", "fivecqure"], ["incepto", "incepto_testing", "incepto_eu"], ["mv", "mv_brazil"], ["fhi", "fhi360"], ["ge_in", "ge"]]
    merge_clients(entries_qxr, qxr_repeated_clients)
    #delete start attribute
    for client in entries_qxr:
        del entries_qxr[client]["start"]
    #add qER clients from oregon database
    cur2 = None
    try:
        conn = connect_hct_beta_database()
        logger.info("I am able to connect to the database.....\n")
    except:
        logger.error('error occured while connecting to database')
    try:
        cur2 = conn.cursor()
        logger.info("Connection Done, Execution in Progress.....\n")
        cur2.execute(
            """SELECT received_on::date, source, total FROM get_db_daily_stats()""")
        logger.info("Execution Done, Adding to table.....\n")
        fill_table(cur2, entries_qer, "qER", "qer_us")
    except:
        logger.exception('error occured when adding qer clients to dicitonary')
    finally:
        if cur2:
            cur2.close()
    #add type attribute to qer clients and delete start attribute for all
    for client in entries_qer:
        del entries_qer[client]["start"]
    resp = {}
    table = []
    #sort table by client with latest upload
    for key, value in entries_qxr.items():
        table.append(value)
        # entry = []
        # for k, v in value.items():
        #     entry.append(v)
        # table.append(entry)
    for key, value in entries_qer.items():
        table.append(value)
        # entry = []
        # for k, v in value.items():
        #     entry.append(v)
        # table.append(entry)
    table = sorted(table, key=lambda x: datetime.strptime(x['last_date'], "%Y-%m-%d"), reverse=True)
    #table = sorted(table, key=lambda x: datetime.strptime(x[4], "%Y-%m-%d"), reverse=True)
    resp['data'] = table
    return JsonResponse(resp)

def get_onpremise(request):
    data = connect_onpremise_csv()
    table = []
    #skip first line because table headers
    i = 1
    lines = re.split("[\r\n]+", data)
    while i < len(lines):
        points = [x.strip() for x in lines[i].split(',')]
        if any(item != "" for item in points):
            type = points[2]
            if type == "qXR":
                table.append({"full_name": points[0], "location": points[1], "qXR": "true", "qER": "false", "qCheck": "false" , "total_uploads": points[3]})
            elif type == "qER":
                table.append({"full_name": points[0], "location": points[1], "qXR": "false", "qER": "true", "qCheck": "false" , "total_uploads": points[3]})
            elif type == "qXR-qCHECK":
                table.append({"full_name": points[0], "location": points[1], "qXR": "true", "qER": "false", "qCheck": "true" , "total_uploads": points[3]})
            elif type == "qER-qCHECK":
                table.append({"full_name": points[0], "location": points[1], "qXR": "false", "qER": "true", "qCheck": "true" , "total_uploads": points[3]})
            #table.append(points)
        i += 1
    table = sorted(table, key=lambda x: x["full_name"])
    #table = sorted(table, key=lambda x: x[0])
    resp = {}
    resp['data'] = table
    return JsonResponse(resp)

def get_map_locations(request):
    resp = {}
    loc = [
        ['India: 5C Network, Baran Government Hospital, CARING, Dr. Biviji, Fujifilm, GE Healthcare, Jankharia Imaging Center, Nikshay, PATH, Prognosys Healthcare, Telerad, Vision X-Ray', 28.7041, 77.1025],
        ['Singapore: Parkway Pantai', 1.3521, 103.8198],
        ['Phillipines: AccessTb, FHI360, Lifetrack, International Organization of Migration', 14.5995, 120.9842],
        ['USA: Ambra, Dr. Irani, Dr. Wiener, MGH, Nuance', 38.9072, -77.0369],
        ['UK: Cimar', 51.5099, -0.1181],
        ['Thailand: Perceptra', 13.7563, 100.5018],
        ['Oman: ICT ImageNet', 23.5880, 58.3829],
        ['France: DCSS, Incepto, Vizyon', 48.8566, 2.3522],
        ['Brazil: MV, CHN', -15.8267, -47.9218],
        ['Russia: Progcom', 55.7512, 37.6184],
        ['Switzerland: FIND', 46.9480, 7.4474],
        ['Malawi: London School of Healthcare and Tropical Medicine', -13.7839, 33.7772],
        ['Malaysia: FOMEMA', 3.1409, 101.6932],
        ['Canada: McGill Healthcare University', 45.4247, -75.6950],
        ['South Korea: Radisen Tech', 37.5326, 127.0246]
    ]
    resp['locations'] = loc
    return JsonResponse(resp)
