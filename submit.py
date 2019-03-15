import sqlite3
from prettytable import from_db_cursor

# copy and paste your SQL queries into each of the below variables
# note: do NOT rename variables

Q1 = '''
select avg(arr_delay) as avg_delay
from flight_delays
'''

Q2 = '''
select max(arr_delay) as max_delay
from flight_delays
'''

Q3 = '''
select carrier, fl_num, origin_city_name, dest_city_name, fl_date
from flight_delays
order by arr_delay desc
limit 1
'''

Q4 = '''
select weekday_name, avg(arr_delay) as average_delay
from flight_delays, weekdays
where weekday_id = day_of_week
group by weekday_name
order by average_delay desc
'''

Q5 = '''
select airline_name, avg(arr_delay) as avg_delay
from flight_delays f, airlines a
where f.airline_id = a.airline_id
and f.airline_id in(
select airline_id 
from flight_delays
where origin = 'SFO')
group by f.airline_id
order by avg_delay desc
'''

Q6 = '''
select cast (count(distinct late.airline_id)*1.0 as float )/ count(distinct f.airline_id) as late_proportion
from flight_delays f, 
(select airline_id 
 from flight_delays 
 group by airline_id 
 having avg(arr_delay)>=10) late
'''

Q7 = '''
select sum (
    ( arr_delay-(select avg(arr_delay) from flight_delays) ) * 
  ( dep_delay-(select avg(dep_delay) from flight_delays) ) )/ (count(*)-1) as cov
 from flight_delays f
'''

Q8 = '''
select delay_of_former.airline_name, max(delay_of_later.delay-delay_of_former.delay) as delay_increase
from airlines a,

(select airline_name, avg(arr_delay) as delay
from flight_delays f,airlines a
where a.airline_id = f.airline_id and f.day_of_month between '1' and '23'
group by airline_name) delay_of_former,

(select airline_name, avg(arr_delay) as delay
from flight_delays f,airlines a
where a.airline_id = f.airline_id and f.day_of_month between '24' and '31'
group by airline_name) delay_of_later

where delay_of_later.airline_name=delay_of_former.airline_name
'''

Q9 = '''
select distinct airline_name
from flight_delays f, airlines a
where origin = 'SFO' and dest = 'PDX' and a.airline_id = f.airline_id
intersect
select distinct airline_name
from flight_delays f, airlines a
where origin = 'SFO' and dest = 'EUG' and a.airline_id = f.airline_id
'''

Q10 = '''
select origin, dest, avg(arr_delay) as avg_delay
from flight_delays f
where (dest = 'SFO' or dest = 'SJC' or dest = 'OAK') and
(origin = 'MDW' or origin = 'ORD') and crs_dep_time > 1400
group by origin, dest
order by avg_delay desc
'''

#################################
# do NOT modify below this line #
#################################

# open a database connection to our local flights database
def connect_database(database_path):
    global conn
    conn = sqlite3.connect(database_path)

def get_all_query_results(debug_print = True):
    all_results = []
    for q, idx in zip([Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10], range(1, 11)):
        result_strings = ("The result for Q%d was:\n%s\n\n" % (idx, from_db_cursor(conn.execute(q)))).splitlines()
        all_results.append(result_strings)
        if debug_print:
            for string in result_strings:
                print string
    return all_results

if __name__ == "__main__":
    connect_database('flights.db')
    query_results = get_all_query_results()