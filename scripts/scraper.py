import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import re
from math import ceil
from snowflake.snowpark.session import Session


professions = ['Dentist', 'CPA', 'Plumber']
locations = ['Olathe,KS', 'Overland Park, KS', 'Leawood,KS', 'Kansas City,KS', 'Kansas City,MO', 'Lees Summit,MO', 'Independence,MO', 'Blue Springs,MO']


connection_parameters = {}
connection_parameters["account"] = 'qnzkqqi-prod'
connection_parameters["database"] = 'COMMERCIAL'
connection_parameters["schema"] = 'LEAD_LISTS'
connection_parameters["warehouse"] = 'COMPUTE_WH'
connection_parameters["role"] = 'SYSADMIN'
connection_parameters["user"] = 'mbjornson'
connection_parameters["Authenticator"] = "externalbrowser"
session = Session.builder.configs(connection_parameters).create()


def YP_SCRAPER(session:Session, professions:list, locations:list) -> str:
    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin
    import requests
    from bs4 import BeautifulSoup
    from urllib.parse import quote_plus
    import re
    from math import ceil
    bus_name = r">[A-Z a-z0-9.,&;'`\-]+<"
    bus_add = r">[A-Z a-z0-9.,&;]+<"
    bus_phone = r">[0-9() -]+<"
    searches = [(x,y) for x in professions for y in locations]
    
    for s in searches:
        df = pd.DataFrame()
        params = {'search_terms': quote_plus(s[0]),
          'geo_location_terms': quote_plus(s[1]), 
          'page' : 1
        }
        print(s[1])
        results = []
        resp = requests.get('http://yellowpages.com/search', params=params)
        content = resp.content
        soup = BeautifulSoup(content, 'html.parser')
        res_ct = ceil(int(re.findall("(?<=of )[0-9]+", str(soup.find_all(class_ = 'showing-count')))[0])/30) if re.findall("(?<=of )[0-9]+", str(soup.find_all(class_ = 'showing-count')))[0] != [] else 0
        #int(re.findall("(?<=[0-9]-)[0-9]+", str(soup.find_all(class_ = 'showing-count')))[0])
        while params['page'] <= res_ct:
            if params['page'] >1:
                resp = requests.get('http://yellowpages.com/search', params=params)
                content = resp.content
                soup = BeautifulSoup(content, 'html.parser')
            res = soup.findAll(class_ = 'info')
            params['page'] +=1
            print(str(params['page'])+' of '+str(res_ct))
            results += res
            temp = pd.DataFrame([{'BUSINESS_NAME': re.sub(r'[<>]', '',re.findall(bus_name,str(x.findAll(class_ = 'business-name')))[0]) if re.findall(bus_name,str(x.findAll(class_ = 'business-name'))) else '' ,
                 'STREET': re.sub(r'[<>]', '',re.findall(bus_add,str(x.findAll(class_ = 'street-address')))[0]) if re.findall(bus_add,str(x.findAll(class_ = 'street-address'))) else '' ,
                 'LOCALITY':re.sub(r'[<>]', '',re.findall(bus_add,str(x.findAll(class_ = 'locality')))[0]) if re.findall(bus_add,str(x.findAll(class_ = 'locality'))) else '' ,
                 'PHONE':re.sub(r'[<>() -]', '',re.findall(bus_phone,str(x.findAll(class_ = 'phones phone primary')))[0]) if re.findall(bus_phone,str(x.findAll(class_ = 'phones phone primary'))) else '', 
                 'SEARCH_TERM': params['search_terms']
                } for x in res if x.findAll(class_ = 'street-address') != []])
            df = pd.concat([df, temp])
        df['LOCALITY'] = df['LOCALITY'].apply(lambda s: re.sub(r'(?<=, [A-Z]{2}) ', ', ', s))
        df['LOCALITY'] = df['LOCALITY'].str.split(',')
        df.to_snowflake('YP_BUSINESS_LIST', if_exists='append', index=False)
    return 'completed'


if __name__ == '__main__': 
   YP_SCRAPER(session, professions, locations)