"""
Creates an example JSON File so one does not have to query the server all the time.
https://ec.europa.eu/jrc/en/PVGIS/tools/hourly-radiation
time is %Y%m%d:%H%M
"""
import requests
import json
r = requests.get(
    "https://re.jrc.ec.europa.eu/api/seriescalc?lat=20&lon=80&pvcalculation=1&peakpower=10&loss=10&outputformat=json")
j = json.loads(r.content)
s = open("additional_datasets/create_pv_profiles/example_india.json", 'x')
s.write(json.dumps(j))
s.close()
