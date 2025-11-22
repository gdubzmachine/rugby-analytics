# scripts/seed_teams_from_list.py

import sys, os, csv
from io import StringIO

from sqlalchemy import text

# Make sure we can import db.connection
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import engine


CSV_DATA = """Name,Shortname,Abbreviation,Country
Argentina,Pumas,ARG,Argentina
Australia,Wallabies,AUS,Australia
England,,ENG,England
Fiji,Flying Fijians,FIJ,Fiji
France,Les Bleus,FRA,France
Ireland,,IRE,Ireland
Italy,Azzurri,ITA,Italy
Japan,Brave Blossoms,JPN,Japan
New Zealand,All Blacks,NZL,New Zealand
Scotland,,SCO,Scotland
South Africa,Springboks,RSA,South Africa
Wales,,WAL,Wales
Canada,Canucks,CAN,Canada
Chile,Los Cóndores,CHI,Chile
Georgia,Lelos,GEO,Georgia
Namibia,Welwitschias,NAM,Namibia
Portugal,Os Lobos,POR,Portugal
Romania,Oaks,ROU,Romania
Samoa,Manu Samoa,SAM,Samoa
Spain,Los Leones,ESP,Spain
Tonga,ʻIkale Tahi,TON,Tonga
United States,Eagles,USA,United States
Uruguay,Los Teros,URU,Uruguay
Belgium,Black Devils,BEL,Belgium
Brazil,Os Tupis,BRA,Brazil
Germany,SchwarzAdler,GER,Germany
Hong Kong,Dragons,HK,Hong Kong
Ivory Coast,Les Éléphants,CIV,Ivory Coast
Kenya,Simbas,KEN,Kenya
Netherlands,Oranje,NED,Netherlands
Paraguay,Yacarés,PAR,Paraguay
Russia,Bears,RUS,Russia
Zimbabwe,Sables,ZIM,Zimbabwe
Andorra,,AND,Andorra
Austria,,AUT,Austria
Bosnia and Herzegovina,,BIH,Bosnia and Herzegovina
Bulgaria,,BUL,Bulgaria
Croatia,,CRO,Croatia
Cyprus,,CYP,Cyprus
Czech Republic,,CZE,Czech Republic
Denmark,,DEN,Denmark
Finland,,FIN,Finland
Greece,,GRE,Greece
Hungary,,HUN,Hungary
Latvia,,LAT,Latvia
Lithuania,,LTU,Lithuania
Luxembourg,,LUX,Luxembourg
Malta,,MLT,Malta
Moldova,,MDA,Moldova
Monaco,,MON,Monaco
Montenegro,,MNE,Montenegro
Norway,,NOR,Norway
Poland,,POL,Poland
Serbia,,SRB,Serbia
Slovakia,,SVK,Slovakia
Slovenia,,SLO,Slovenia
Sweden,,SWE,Sweden
Switzerland,,SUI,Switzerland
Ukraine,,UKR,Ukraine
Bahamas,,BAH,Bahamas
Barbados,,BAR,Barbados
Bermuda,,BER,Bermuda
British Virgin Islands,,IVB,British Virgin Islands
Cayman Islands,,CAY,Cayman Islands
Colombia,Los Tucanes,COL,Colombia
Costa Rica,,CRC,Costa Rica
Guatemala,,GUA,Guatemala
Guyana,Green Machine,GUY,Guyana
Jamaica,,JAM,Jamaica
Mexico,Los Serpientes,MEX,Mexico
Panama,,PAN,Panama
Peru,Los Tumis,PER,Peru
Saint Lucia,,LCA,Saint Lucia
Saint Vincent and the Grenadines,,VIN,Saint Vincent and the Grenadines
Trinidad and Tobago,,TTO,Trinidad and Tobago
Venezuela,Los Tucanes (Venezuela),VEN,Venezuela
Algeria,Les Lionceaux,ALG,Algeria
Botswana,The Vultures,BOT,Botswana
Burundi,,BDI,Burundi
Burkina Faso,,BFA,Burkina Faso
Cameroon,Indomitable Lions,CMR,Cameroon
DR Congo,Leopards,COD,DR Congo
Egypt,,EGY,Egypt
Eswatini,Imiphiva,SWZ,Eswatini
Ghana,Black Eagles,GHA,Ghana
Lesotho,,LES,Lesotho
Madagascar,Les Makis,MAD,Madagascar
Mali,Les Aigles du Mali,MLI,Mali
Mauritius,,MRI,Mauritius
Morocco,Atlas Lions,MAR,Morocco
Nigeria,Black Stallions,NGR,Nigeria
Rwanda,Silverbacks,RWA,Rwanda
Senegal,Les Lions de la Téranga,SEN,Senegal
Tanzania,,TAN,Tanzania
Togo,,TOG,Togo
Tunisia,Les Aigles de Carthage,TUN,Tunisia
Uganda,Uganda Cranes,UGA,Uganda
Zambia,Chipolopolo,ZAM,Zambia
Azerbaijan,,AZE,Azerbaijan
China,,CHN,China
Chinese Taipei,,TPE,Chinese Taipei (Taiwan)
India,,IND,India
Iran,,IRN,Iran
Israel,,ISR,Israel
Jordan,,JOR,Jordan
Kazakhstan,,KAZ,Kazakhstan
Kyrgyzstan,,KGZ,Kyrgyzstan
South Korea,Brave Tigers,KOR,South Korea
Laos,,LAO,Laos
Lebanon,,LBN,Lebanon
Mongolia,,MGL,Mongolia
Nepal,,NEP,Nepal
Pakistan,,PAK,Pakistan
Qatar,,QAT,Qatar
Syria,,SYR,Syria
Sri Lanka,Tuskers,SRI,Sri Lanka
Thailand,,THA,Thailand
Turkey,,TUR,Turkey
United Arab Emirates,,UAE,United Arab Emirates
Uzbekistan,,UZB,Uzbekistan
American Samoa,,ASA,American Samoa
Brunei,,BRU,Brunei
Cook Islands,,COK,Cook Islands
Guam,,GUM,Guam
Indonesia,,INA,Indonesia
Malaysia,,MAS,Malaysia
Niue,,NIU,Niue
Papua New Guinea,Pukpuks,PNG,Papua New Guinea
Philippines,Volcanoes,PHI,Philippines
Singapore,The Blacks,SGP,Singapore
Solomon Islands,,SOL,Solomon Islands
Vanuatu,,VAN,Vanuatu
Blues,Blues,BLU,New Zealand
Chiefs,Chiefs,CHI,New Zealand
Crusaders,Crusaders,CRU,New Zealand
Highlanders,Highlanders,HIG,New Zealand
Hurricanes,Canes,HUR,New Zealand
ACT Brumbies,Brumbies,BRU,Australia
Queensland Reds,Reds,RED,Australia
NSW Waratahs,Waratahs,WAR,Australia
Melbourne Rebels,Rebels,REB,Australia
Western Force,Force,FOR,Australia
Bulls,Bulls,BUL,South Africa
Sharks,Sharks,SHA,South Africa
Stormers,Stormers,STO,South Africa
Lions,Lions,LIO,South Africa
Cheetahs,Cheetahs,CHE,South Africa
Southern Kings,Kings,KIN,South Africa
Cats,Cats,CAT,South Africa
Jaguares,Jaguares,JAG,Argentina
Sunwolves,Sunwolves,SUN,Japan
Fijian Drua,Drua,DRU,Fiji
Moana Pasifika,Moana Pasifika,MOA,Pacific Islands
Griquas,Griquas,GRI,South Africa
Pumas (Mpumalanga),Pumas,PUM,South Africa
Leinster Rugby,Leinster,LEI,Ireland
Munster Rugby,Munster,MUN,Ireland
Ulster Rugby,Ulster,ULS,Ireland
Connacht Rugby,Connacht,CON,Ireland
Edinburgh Rugby,Edinburgh,EDI,Scotland
Glasgow Warriors,Glasgow GWS,GLA,Scotland
Border Reivers,Borders,BOR,Scotland
Bridgend RFC,Bridgend,BRI,Wales
Caerphilly RFC,Caerphilly,CAE,Wales
Cardiff RFC (pre-2003),Cardiff,CAR,Wales
Ebbw Vale RFC,Ebbw Vale,EBB,Wales
Llanelli RFC (pre-Scarlets),Llanelli,LLA,Wales
Neath RFC,Neath,NEA,Wales
Newport RFC,Newport,NEW,Wales
Pontypridd RFC,Pontypridd,PON,Wales
Swansea RFC,Swansea,SWA,Wales
Cardiff Blues (Cardiff Rugby),Cardiff Blues,CBL,Wales
Dragons (Newport Gwent Dragons),Dragons,DRA,Wales
Ospreys (Neath-Swansea),Ospreys,OSP,Wales
Scarlets (Llanelli Scarlets),Scarlets,SCA,Wales
Celtic Warriors,Celtic Warriors,CEL,Wales
Benetton Rugby Treviso,Benetton,BEN,Italy
Aironi,Aironi,AIR,Italy
Zebre Parma,Zebre,ZEB,Italy
Toyota Cheetahs,Cheetahs,CHE,South Africa
Southern Kings,Kings,KIN,South Africa
Bulls,Bulls,BUL,South Africa
Lions,Lions,LIO,South Africa
Sharks,Sharks,SHA,South Africa
Stormers,Stormers,STO,South Africa
Stade Toulousain,Toulouse,ST,France
Stade Français Paris,Stade Français,SFP,France
Racing 92,Racing 92,R92,France
ASM Clermont Auvergne,Clermont,ASM,France
Castres Olympique,Castres,CO,France
Biarritz Olympique,Biarritz,BO,France
USA Perpignan,Perpignan,USAP,France
Aviron Bayonnais,Bayonne,AB,France
Section Paloise,Pau,PAU,France
CA Brive,Brive,CAB,France
SU Agen,Agen,SUA,France
FC Grenoble,Grenoble,FCG,France
RC Toulonnais,Toulon,RCT,France
Union Bordeaux Bègles,Bordeaux Bègles,UBB,France
Lyon OU,Lyon,LOU,France
Montpellier Hérault Rugby,Montpellier,MHR,France
AS Béziers Hérault,Béziers,ASB,France
RC Narbonne,Narbonne,RCN,France
US Dax,Dax,USD,France
US Colomiers,Colomiers,USC,France
Stade Rochelais,La Rochelle,SR,France
Stade Montois,Mont-de-Marsan,SM,France
CS Bourgoin-Jallieu,Bourgoin,CSBJ,France
US Montauban,Montauban,USM,France
SC Albi,Albi,SCA,France
Oyonnax Rugby,Oyonnax,USO,France
FC Lourdes,Lourdes,FCL,France
Stadoceste Tarbais (Tarbes),Tarbes,STPR,France
Bath Rugby,Bath,BAT,England
Bristol Bears,Bristol,BRI,England
Exeter Chiefs,Exeter,EXE,England
Gloucester Rugby,Gloucester,GLO,England
Harlequins,Quins,HAR,England
Leicester Tigers,Leicester,LEI,England
Newcastle Falcons,Newcastle,NEW,England
Northampton Saints,Northampton,NOR,England
Sale Sharks,Sale,SAL,England
Saracens,Saracens,SAR,England
London Wasps (Wasps),Wasps,WAS,England
London Irish,Irish,LDI,England
Worcester Warriors,Worcester,WOR,England
London Welsh,London Welsh,LON,England
Leeds Tykes (Yorkshire Carnegie),Leeds,YKS,England
Rotherham Titans,Rotherham,ROT,England
Richmond RFC,Richmond,RCH,England
London Scottish,London Scottish,LOS,England
West Hartlepool,West Hartlepool,WHL,England
Moseley RFC,Moseley,MOS,England
Orrell R.U.F.C.,Orrell,ORL,England
Nottingham R.F.C.,Nottingham,NOT,England
Waterloo R.F.C.,Waterloo,WAT,England
Bedford Blues,Bedford,BED,England
Rosslyn Park,Rosslyn Park,ROS,England
Liverpool St Helens,Liverpool S.H.,LSH,England
Rugby Lions,Rugby Lions,RUG,England
Coventry R.F.C.,Coventry,COV,England
"""


def main():
    reader = csv.DictReader(StringIO(CSV_DATA))
    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for row in reader:
            name = (row.get("Name") or "").strip()
            if not name:
                continue  # skip empty lines

            short_name = (row.get("Shortname") or "").strip() or None
            abbr = (row.get("Abbreviation") or "").strip() or None
            country = (row.get("Country") or "").strip() or None

            # Skip if team already exists by name
            existing = conn.execute(
                text("SELECT team_id FROM teams WHERE name = :name"),
                {"name": name},
            ).scalar_one_or_none()

            if existing:
                print(f"Skipping existing team: {name} (id={existing})")
                skipped += 1
                continue

            conn.execute(
                text(
                    """
                    INSERT INTO teams (name, short_name, abbreviation, country, espn_team_id)
                    VALUES (:name, :short_name, :abbreviation, :country, NULL)
                    """
                ),
                {
                    "name": name,
                    "short_name": short_name,
                    "abbreviation": abbr,
                    "country": country,
                },
            )
            print(f"Inserted team: {name}")
            inserted += 1

    print(f"\nDone. Inserted {inserted} teams, skipped {skipped} existing.")


if __name__ == "__main__":
    main()
