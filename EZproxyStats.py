import re, glob, os, logging
from datetime import date, datetime
import csv
import pandas as pd
from matplotlib import pyplot as plt
import geoip2.database
from urllib.parse import urlparse

# Setup logging for debugging and error handling
logging.basicConfig(level=logging.INFO, filename='log_processing.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Function to clean URLs using urllib.parse
def clean_url(url):
    parsed = urlparse(url)
    return parsed.netloc

# Setup
tdy = str(date.today())
year = tdy[2:4]
m = int(tdy[5:7]) - 1
if len(str(m)) == 1:
    month = '0' + str(m)
else:
    month = str(m)
stats = 'EZproxy_' + month + year
stats_title = month + '/' + year

statdirs = 'C:\\Statistics\\' + stats
chartdirs = 'C:\\Statistics\\' + stats + '\\charts\\'

os.makedirs(statdirs, exist_ok=True)
os.makedirs(chartdirs, exist_ok=True)

statfile = 'C:\\Statistics\\' + stats + '\\' + stats + '.csv'
htmlfile = 'C:\\Statistics\\' + stats + '\\' + stats + '.html'

output = open(statfile, 'w')

dbfile = 'C:\\Statistics\\dblist.csv'
db_reader = csv.reader(open(dbfile, 'r'))
dbs = {}

for db_row in db_reader:
    a, b = db_row
    dbs[a] = b

dblist = list(dbs.keys())

weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
months = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6, 'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

columns = ["Date", "Weekday", "Hour", "Country", "State", "City", "Location", "Status", "Requested_url", "Referring_url"]
output.write(",".join(columns))

ezproxy_logs = 'C:\\Statistics\\ezproxy_logs\\'
geoip_db_path = "C:\\GeoLite2-City.mmdb"  # Adjust this path to your GeoLite2 database

# Open GeoIP2 database once for efficiency
try:
    with geoip2.database.Reader(geoip_db_path) as reader:
        # Process log files
        for filename in glob.glob(os.path.join(ezproxy_logs, '*')):
            lines = [line.strip() for line in open(filename)]
            for line in lines:
                try:
                    # Parse the log line
                    log = line.split(" ", 5)  # Split into 6 fields
                    ip = log[0]
                    timestamp = log[1].strip('[]')
                    username = log[2] if log[2] != '-' else 'anonymous'
                    requested_url = log[3]
                    referring_url = log[5].strip() if log[5] != '-' else 'No Referrer'

                    # GeoIP lookup
                    try:
                        response = reader.city(ip)
                        country = response.country.name or 'Unknown'
                        state = response.subdivisions.most_specific.name or 'Unknown'
                        city = response.city.name or 'Unknown'
                    except geoip2.errors.AddressNotFoundError:
                        country = 'Unknown'
                        state = 'Unknown'
                        city = 'Unknown'
                    except Exception as e:
                        logging.error(f"GeoIP lookup error for IP: {ip}. Error: {e}")
                        country = 'Error'
                        state = 'Error'
                        city = 'Error'

                    # Parse timestamp
                    ts = timestamp.split('/')
                    ddate = ts[0]
                    month = ts[1]
                    year_hour = ts[2]
                    year = year_hour.split(":")[0]
                    hour = year_hour.split(":")[1]
                    weekday = datetime.strptime(f"{year}-{month}-{ddate}", "%Y-%b-%d").strftime("%A")

                    # Determine location
                    location = "On Campus" if ip.startswith("10.") else "Off Campus"

                    # Clean URLs
                    requested_url = clean_url(requested_url)
                    referring_url = clean_url(referring_url)

                    # Write to output file
                    output.write('\n')
                    output.write(
                        f"{ddate},{weekday},{hour},{country},{state},{city},{location},anonymous,{requested_url},{referring_url}"
                    )

                except Exception as e:
                    logging.error(f"Error processing log line: {line}. Error: {e}")
except FileNotFoundError as e:
    logging.critical(f"GeoIP database file not found: {geoip_db_path}. Error: {e}")
except Exception as e:
    logging.critical(f"Unexpected error opening GeoIP database: {e}")

output.close()
# Process stats for charts and HTML report generation
df = pd.read_csv(statfile)

html = open(htmlfile, 'w')
html.write('<html><head><title>EZproxy Logfile Analysis - ' + stats_title + '</title>')
html.write('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.4/css/bootstrap.min.css">')
html.write('<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/js/bootstrap.min.js"></script>')
html.write('<style>table{width:65%;}th{display:none;}td{text-align:center;}</style></head>')
html.write('<body><div class="container"><h1>EZproxy Logfile Analysis - ' + stats_title + '</h1><br><br>')

# List of predefined charts
charts = [
    ('byday', 'Date', 'Sessions', 'EZproxy Sessions by Day of Month'),
    ('byweekday', 'Weekday', 'Sessions', 'EZproxy Sessions by Weekday'),
    ('byhour', 'Hour', 'Sessions', 'EZproxy Sessions by Hour'),
    ('bycountry', 'Country', 'Sessions', 'EZproxy Sessions by Country'),
    ('bystate', 'State', 'Sessions', 'EZproxy Sessions by State'),
    ('bycity', 'City', 'Sessions', 'EZproxy Sessions by City'),
]

# Generate predefined charts
for filename, group_by, count_column, title in charts:
    group = df.groupby(group_by).size().reset_index(name=count_column)
    group.sort_values(count_column, ascending=False, inplace=True)

    plt.figure(figsize=(10, 6))
    plt.barh(group[group_by], group[count_column], alpha=0.75)
    plt.title(title)
    plt.tight_layout()
    chart_path = f'{chartdirs}{filename}.png'
    plt.savefig(chart_path)
    plt.close()

    html.write(f'<div><h2>{title}</h2><img src="charts/{filename}.png"></div>')

# Generate On Campus vs Off Campus Users chart
location_counts = df['Location'].value_counts()

plt.figure(figsize=(8, 6))
location_counts.plot(kind='bar', alpha=0.75, color=['#1f77b4', '#ff7f0e'])
plt.title('On Campus vs Off Campus Users')
plt.ylabel('Number of Users')
plt.xlabel('Location')
plt.xticks(rotation=0)
plt.tight_layout()

chart_location_path = os.path.join(chartdirs, 'on_off_campus_users.png')
plt.savefig(chart_location_path)
plt.close()

# Embed On Campus vs Off Campus Users chart in HTML
html.write('<div class="row"><center><h2>On Campus vs Off Campus Users</h2>')
html.write(f'<img src="charts/on_off_campus_users.png" alt="On Campus vs Off Campus Users Chart" /><br><br></center></div>')

# Close HTML file
html.write('</div></body></html>')
html.close()