import pandas as pd
from pyproj import Transformer
from pyproj import CRS
import pyproj

# clean the finess_geocoded_latest.csv from 
# https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/
# derniere mise a jour 8 janvier 2021

def dataRaw():
    # Define Variables
    header = [
        'section', 'nofinesset', 'nofinessej', 'rs', 'rslongue','complrs', 'compldistrib',
        'numvoie', 'typvoie', 'voie','compvoie','lieuditbp', 'commune','departement',
        'libdepartement', 'ligneacheminement', 'telephone', 'telecopie', 'categetab',
        'libcategetab', 'categagretab', 'libcategagretab', 'siret', 'codeape',
        'codemft', 'libmft', 'codesph', 'libsph', 'dateouv', 'dateautor', 'maj','numuai'
    ]

    geoloc_names = [
        'nofinesset', 'coordxet', 'coordyet', 'sourcecoordet', 'datemaj'
    ]

    file = './finess_geocoded_latest.csv'
    #file = 'etalab-cs1100507-stock-20210108-0427.csv'

    try:
        df_raw = pd.read_csv(file, encoding ='latin1', sep=";", dtype=str, skiprows=1, header=None, names=header)
        # Take care 'departement'
        df_raw.loc[df_raw['libdepartement'] == 'GUADELOUPE', 'departement'] = '971'
        df_raw.loc[df_raw['libdepartement'] == 'MARTINIQUE', 'departement'] = '972'
        df_raw.loc[df_raw['libdepartement'] == 'GUYANE', 'departement'] = '973'
        df_raw.loc[df_raw['libdepartement'] == 'LA REUNION', 'departement'] = '974'
        df_raw.loc[df_raw['libdepartement'] == 'SAINT PIERRE ET MIQUELON', 'departement'] = '975'
        df_raw.loc[df_raw['libdepartement'] == 'MAYOTTE', 'departement'] = '976'
        # Data Geoloc
        geoloc_raw = df_raw[df_raw["section"] == "geolocalisation"]
        df_geoloc_raw = geoloc_raw.drop(columns=['section'])
        geoloc = df_geoloc_raw.drop(columns=geoloc_raw.columns[6:])
        geoloc.rename(columns=lambda x: geoloc_names[list(geoloc.columns).index(x)], inplace=True)
        geoloc.reset_index(drop = True, inplace = True)
        # Data ES
        df_es_raw = df_raw[df_raw["section"] == "structureet"]
        df_es = df_es_raw.drop(columns=['section'])
        # Data Raw
        df_es['nofinesset'] = df_es['nofinesset'].astype(str)
        geoloc['nofinesset'] = geoloc['nofinesset'].astype(str)
        df_raw = df_es.merge(geoloc, on='nofinesset', how='left')
        len_raw = len(df_raw)
        # Write file utf-8
        df_raw.to_csv('RawfinessClean.csv', encoding='utf-8', sep=';')
    except Exception as e:
        print (type(e))
        print(e)
    finally:
        # verification
        len_df = len(pd.read_csv('./RawfinessClean.csv',dtype=str, sep=';'))
        if len_df == len_raw:
            try:
                print(f'dataRaw fin {len_raw}, {df_raw.shape}')
                return(df_raw)
            except Exception as e:
                print(e)
            else:
                print(f'len_df {len_df}, len_raw {len_raw},  ')   

def dataTransform(df_raw):
    print('df_raw1')
    '''
    Informations sur la Géo-localisation : Le système d’information source contenant les coordonnées géographiques permettant 
    de géo-localiser les établissements répertoriés dans FINESS est le produit BD-ADRESSE en version 2.1 de 
    l’IGN (Institut Géographique National).

    ZONE SYSTÈME GEODESIQUE ELLIPSOÏDE ASSOCIEE PROJECTION
    //France métropolitaine : RGF93 IAG GRS 1980 Coniques conformes 9 zones - Lambert 93
    //Guadeloupe, Martinique : WGS84 IAG GRS 1980 UTM Nord fuseau 20
    //Guyane : RGFG95 IAG GRS 1980 UTM Nord fuseau 22
    //Réunion : RGR92 IAG GRS 1980 UTM Sud fuseau 40
    //Mayotte : RGM04 IAG GRS 1980 UTM Sud fuseau 38
    //Saint Pierre et Miquelon : RGSPM06 IAG GRS 1980 UTM Nord fuseau 21
    '''
    df_cols = [
    'Numero FINESS ET', 'Numero FINESS EJ','Raison sociale', 'Raison sociale longue', 'Complement de raison sociale', 'Complement de distribution',
    'Numero de voie', 'Type de voie', 'Libelle de voie', 'Complément de voie', 'Lieu-dit / BP', 'Code Commune', 'Departement', 'Libelle departement',
    'Ligne d’acheminement (CodePostal+Lib commune)', 'Telephone', 'Telecopie', 'Categorie d’etablissement', 'Libelle categorie d’etablissement',
    'Categorie d’agregat d’etablissement', 'Libelle categorie d’agregat d’etablissement', 'Numero de SIRET', 'Code APE', 'Code MFT', 'Libelle MFT',
    'Code SPH', 'Libelle SPH', 'Date d’ouverture', 'Date d’autorisation', 'Date de mise à jour sur la structure', 'Numero éducation nationale',
    'CoordX', 'CoordY', 'Source des coordonnées', 'Date de mise à jour des coordonnées', 'Code Postal', 'Commune'
]
    try:
        df_final= df_raw.astype(str)
        print('df_raw2')
        df_final[['coordxet', 'coordyet']] = df_final[['coordxet', 'coordyet']].apply(pd.to_numeric, errors='coerce')
        print('df_raw3')
        df_final[['Code', 'Ville']] = df_final['ligneacheminement'].str.extract('(\d+)\s([\w+ ]*$)', expand=True)

        df_final['Ville'] = df_final['Ville'].str.replace('CEDEX', '')
        df_final['Ville'] = df_final['Ville'].str.replace('\d+', '')


        df_final.columns = df_cols
        df_final['CRS']= df_final['Source des coordonnées'].apply(lambda x:x.rsplit(',', 1)[1])
        df_final.to_csv('./finessClean.csv', encoding='utf-8', sep=';')
        print('df_raw4')

        len_final = len(df_final)

    except Exception as e:
        print (type(e))
        print(e)
    finally:
        # verification
        print('df_raw5')
        len_df = len(pd.read_csv('./finessClean.csv',dtype=str, sep=';'))
        if len_df == len_final:
            print('df_raw6')
            try:
                print(f'dataFiness fin {len_final}')
                return(df_final)

            except Exception as e:
                print(e)
            else:
                print(f'len_df {len_df}, len_final {len_final} ')  
    
def dataCrs(df_finess):
    try:
        # Use USPG ti find the good projection: https://epsg.org/home.html
        EPSG_dict = {"LAMBERT_93":2154, "UTM_N20":4559, "UTM_N21":4467, "UTM_N22":2972, "UTM_S38":4471, "UTM_S40":2975}
        df_finess["EPSG"] = df_finess["CRS"].map(EPSG_dict)

        crs = [2154, 4559, 4467, 2972, 4471, 2975]
        #https://github.com/pyproj4/pyproj/blob/master/docs/examples.rst
        df_crs = pd.DataFrame()
        for c in crs:
            df = df_finess.loc[df_finess["EPSG"] == c, :].copy()
            crs = CRS.from_epsg(c)
            proj = Transformer.from_crs(crs, crs.geodetic_crs, always_xy=True)
            xx, yy = proj.transform(df["CoordX"].values, df["CoordY"].values)
            df["Longitude"] = xx
            df["Latitude"] = yy
            df_crs = df_crs.append(df)
        #df_crs.replace(to_replace='nan', value="Unknow", inplace=True)
        list_drop = [
            'Complement de raison sociale', 'Complement de distribution', 'Type de voie','Libelle de voie',
             'Complément de voie','Lieu-dit / BP', 'Ligne d’acheminement (CodePostal+Lib commune)','Telephone',             
             'Telecopie','Source des coordonnées', 'Date de mise à jour des coordonnées', 
             'Numero de voie', 'Libelle de voie', 'Complément de voie',
             'CRS', 'EPSG', 'CoordX', 'CoordY', 'Raison sociale longue'
             ]
        df_crs_clean = df_crs.drop(list_drop, axis =1 )
        df_crs_clean.loc[df_crs_clean.eq("nan").any(1), :]
        #order columns
        cols = list(df_crs_clean)
        cols_etablissement = cols[:4] + cols[7:21]
        cols_address = cols[4:7] + cols[23:25]
        cols_geo = cols[21:23] + cols[25:]
        new_cols = cols_etablissement + cols_address[3:] + cols_address[:3] + cols_geo 
        df_clean = df_crs_clean[new_cols]

        
        # sauvegarde en utf-8
        df_clean.to_csv('./finessCrsClean.csv', encoding='utf-8', sep=';')
        len_final = len(df_clean)
    except Exception as e:
        print (type(e))
        print(e)
    finally:
            # verification
            len_df = len(pd.read_csv('./finessCrsClean.csv',dtype=str, sep=';'))
            if len_df == len_final:
                try:
                    print(f'dataCrs fin {len_final}, {df_clean.shape}')
                    return(df_crs)
                except Exception as e:
                    print(e)
                else:
                    print(f'len_df {len_df}, len_final {len_final},  ') 


def main():
    df_raw = dataRaw()
    df_finess = dataTransform(df_raw)
    df_crs= dataCrs(df_finess)

    return()


if __name__ =="__main__":
    main()