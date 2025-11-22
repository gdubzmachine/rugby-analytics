# scripts/seed_venues_from_list.py
# -*- coding: utf-8 -*-

import sys
import os
from io import StringIO
import csv
from sqlalchemy import text

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connection import engine

# ----------------------------------------------------------------------
# Embedded CSV data (all your venues with real coordinates)
# ----------------------------------------------------------------------
CSV_DATA = """name,city,country,latitude,longitude
Twickenham Stadium,London,England,51.455990,-0.342329
Stade de France,Saint-Denis,France,48.924454,2.359665
Principality Stadium,Cardiff,Wales,51.478146,-3.183414
Stadio Olimpico,Rome,Italy,41.933964,12.454297
Murrayfield Stadium,Edinburgh,Scotland,55.942307,-3.241998
Aviva Stadium,Dublin,Ireland,53.335174,-6.227952
Croke Park,Dublin,Ireland,53.360900,-6.251900
Stadio Flaminio,Rome,Italy,41.926933,12.472308
Stade Vélodrome,Marseille,France,43.269852,5.395799
Stade Pierre-Mauroy,Lille,France,50.611835,3.130006
Parc Olympique Lyonnais,Lyon,France,45.765293,4.981832
Parc y Scarlets,Llanelli,Wales,51.679170,-4.129170
Estadio José Amalfitani,Buenos Aires,Argentina,-34.635375,-58.520711
Queensland Country Bank Stadium,Townsville,Australia,-19.266110,146.816670
Optus Stadium,Perth,Australia,-31.951110,115.889170
Allianz Stadium,Sydney,Australia,-33.889170,151.225280
Wellington Regional Stadium,Wellington,New Zealand,-41.273060,174.785830
Eden Park,Auckland,New Zealand,-36.875035,174.744057
Ellis Park Stadium,Johannesburg,South Africa,-26.197500,28.060830
Cape Town Stadium,Cape Town,South Africa,-33.903330,18.411110
Kings Park Stadium,Durban,South Africa,-29.825000,31.029720
Loftus Versfeld Stadium,Pretoria,South Africa,-25.753330,28.222780
Suncorp Stadium,Brisbane,Australia,-27.464867,153.009186
Stadium Australia,Sydney,Australia,-33.847220,151.063060
Forsyth Barr Stadium,Dunedin,New Zealand,-45.869170,170.524440
Waikato Stadium,Hamilton,New Zealand,-37.781528,175.269363
Orangetheory Stadium,Christchurch,New Zealand,-43.543700,172.604100
Nelson Mandela Bay Stadium,Port Elizabeth,South Africa,-33.937780,25.598890
Mbombela Stadium,Nelspruit,South Africa,-25.461000,30.929000
Estadio Malvinas Argentinas,Mendoza,Argentina,-32.887000,-68.874000
Estadio Mario Alberto Kempes,Córdoba,Argentina,-31.368000,-64.241100
Marvel Stadium,Melbourne,Australia,-37.816528,144.947266
Adelaide Oval,Adelaide,Australia,-34.915560,138.596110
Stade de Bordeaux,Bordeaux,France,44.897335,-0.561928
Stade de la Beaujoire,Nantes,France,47.256008,-1.524965
Stade Geoffroy-Guichard,Saint-Étienne,France,45.460506,4.389225
Allianz Riviera,Nice,France,43.705173,7.192550
Stadium de Toulouse,Toulouse,France,43.583060,1.434170
RDS Arena,Dublin,Ireland,53.332300,-6.229800
Thomond Park,Limerick,Ireland,52.673060,-8.642500
Kingspan Stadium,Belfast,Northern Ireland,54.582800,-5.927800
The Sportsground,Galway,Ireland,53.296700,-9.040200
Cardiff Arms Park,Cardiff,Wales,51.482100,-3.184400
Liberty Stadium,Swansea,Wales,51.642000,-3.935000
Rodney Parade,Newport,Wales,51.588000,-2.991000
Scotstoun Stadium,Glasgow,Scotland,55.881400,-4.339700
DAM Health Stadium,Edinburgh,Scotland,55.942300,-3.242000
Stadio Monigo,Treviso,Italy,45.691900,12.217800
Stadio Sergio Lanfranchi,Parma,Italy,44.826100,10.339400
Mount Smart Stadium,Auckland,New Zealand,-36.918330,174.812500
GIO Stadium,Canberra,Australia,-35.239500,149.064700
AAMI Park,Melbourne,Australia,-37.825000,144.983500
HBF Park,Perth,Australia,-31.944400,115.871300
HFC Bank Stadium,Suva,Fiji,-18.150000,178.442800
Chichibunomiya Rugby Stadium,Tokyo,Japan,35.672603,139.718169
National Stadium,Singapore,Singapore,1.304410,103.874260
Free State Stadium,Bloemfontein,South Africa,-29.117220,26.208890
Newlands Stadium,Cape Town,South Africa,-33.970560,18.468330
Stade Chaban-Delmas,Bordeaux,France,44.829170,-0.597780
Stade de Gerland,Lyon,France,45.723890,4.832220
Stade Ernest-Wallon,Toulouse,France,43.621940,1.415560
Stade Marcel-Deflandre,La Rochelle,France,46.158060,-1.178060
Stade Jean-Bouin,Paris,France,48.843060,2.252780
Paris La Défense Arena,Nanterre,France,48.895800,2.230200
Stade Mayol,Toulon,France,43.118890,5.936670
Stade Jean-Dauger,Bayonne,France,43.486050,-1.480220
Stade du Hameau,Pau,France,43.309440,-0.316940
Stade Aimé Giral,Perpignan,France,42.715280,2.891390
Stade Pierre-Fabre,Castres,France,43.610830,2.252780
Stade Marcel-Michelin,Clermont-Ferrand,France,45.789330,3.106150
GGL Stadium,Montpellier,France,43.593060,3.849720
Stade Charles-Mathon,Oyonnax,France,46.253610,5.644720
Recreation Ground,Bath,England,51.382220,-2.355280
Ashton Gate,Bristol,England,51.439880,-2.621020
Sandy Park,Exeter,England,50.709440,-3.467500
Kingsholm Stadium,Gloucester,England,51.871221,-2.243151
Twickenham Stoop,London,England,51.450905,-0.343018
Welford Road Stadium,Leicester,England,52.624170,-1.133060
Kingston Park,Newcastle upon Tyne,England,55.018715,-1.673066
Franklin's Gardens,Northampton,England,52.240520,-0.919190
AJ Bell Stadium,Salford,England,53.469090,-2.378830
StoneX Stadium,London,England,51.603610,-0.223610
Coventry Building Society Arena,Coventry,England,52.448060,-1.495560
Sixways Stadium,Worcester,England,52.215560,-2.162500
Brentford Community Stadium,Brentford,England,51.490715,-0.289048
"""

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    reader = csv.DictReader(StringIO(CSV_DATA))
    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for row in reader:
            name = row["name"].strip()
            city = row["city"].strip()
            country = row["country"].strip()
            lat = float(row["latitude"])
            lon = float(row["longitude"])

            # Check if venue already exists by name
            exists = conn.execute(
                text("SELECT 1 FROM venues WHERE name = :name"),
                {"name": name}
            ).fetchone()

            if exists:
                print(f"Skipping duplicate: {name}")
                skipped += 1
                continue

            # Insert new venue
            conn.execute(
                text(
                    """
                    INSERT INTO venues 
                        (name, city, country, latitude, longitude, espn_venue_id)
                    VALUES 
                        (:name, :city, :country, :lat, :lon, NULL)
                    """
                ),
                {
                    "name": name,
                    "city": city,
                    "country": country,
                    "lat": lat,
                    "lon": lon,
                }
            )
            print(f"Inserted: {name}")
            inserted += 1

    print(f"\nDone! Inserted {inserted} new venues, skipped {skipped} duplicates.")


if __name__ == "__main__":
    main()
