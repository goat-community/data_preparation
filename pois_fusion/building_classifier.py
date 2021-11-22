#%%

import geopandas as gpd
import pandas as pd
import os
import sys
import numpy as np
from sklearn import svm
from sklearn.model_selection import train_test_split
from sklearn.metrics import cohen_kappa_score
from joblib import dump, load

#%%

def building_classifier(out_name):
    '''
    use labeled dataset to train and optimzie model. save the model for further prediction
    '''
    # residential_types = ["apartments","bungalow","detached","dormitory","residential","house","terrace","home","semidetached_house"]
    residential_types = ['Wohnhaus','Gemischt genutztes Gebäude mit Wohnen','Ferienhaus','Wohngebäude mit Handel und Dienstleistungen',
    'Wochenendhaus','Gemischt genutztes Gebäude mit Wohnen;Gaststätte','Gemischt genutztes Gebäude mit Wohnen;Arztpraxis',
    'Wohngebäude', 'Gebäude für Handel und Dienstleistung mit Wohnen','Wohn- und Bürogebäude',
    'Gebäude für Gewerbe und Industrie mit Wohnen','Wohn- und Geschäftsgebäude','Wohn- und Betriebsgebäude']
    # Gebäude für öffentliche Zwecke mit Wohnen'

    # buildings = gpd.read_file(os.path.join(sys.path[0],"data","buildings" + ".geojson"))
    buildings = gpd.read_file(os.path.join(sys.path[0],"data","ave_GebaeudeBauwerk.sqlite"), encoding='utf-8')
    # change projection to Cartesian system
    buildings = buildings.to_crs({'init': 'epsg:3857'})
    #delete columns:
    # delete_column = [2,3,5,7,9,10,11,12,13,14,17, 19, 22, 24, 29, 30, 31, 33, 34, 37]
    # buildings = buildings.drop(buildings.columns[delete_column], axis=1)

    '''
    Shape analysis of building footprint
    quantatative measure of shape: 
    0. area, 1. Compactness, 2. convexity, 3. solidality, 4. roundness
    '''
    buildings['area'] = buildings['geometry'].area
    # geometry have polygon and linestring, remove the linestring geometry
    buildings = buildings[buildings.area != 0]
    buildings['perimeter'] = buildings['geometry'].length
    buildings['compactness'] = (buildings['area']*4*np.pi)/buildings['perimeter']**2    # compactness
    buildings['convex'] = buildings['geometry'].convex_hull
    buildings['peri_convex'] = buildings['convex'].length
    buildings['area_convex'] = buildings['convex'].area
    buildings['convexity'] = buildings['peri_convex']/buildings['perimeter']    # convexity
    buildings['solidality'] = buildings['area']/buildings['area_convex']     # solidality
    buildings['roundness'] = 4*np.pi*buildings['area']/buildings['peri_convex']**2    # roundness
  
    # extract training and testing features
    buildings['type'] = buildings['funktion'].apply( lambda x: 1 if x in residential_types else 3 if x=="yes" else 2 )
    # seperate dataset ramdonly
    train, test = train_test_split(buildings, test_size=0.8)   # only use 20% of data as training dataset
    train_feature = np.array(train[["area","compactness","convexity","solidality", "roundness"]]).tolist()
    test_feature = np.array(test[["area","compactness","convexity","solidality", "roundness"]]).tolist()
    train_label = train['type'].tolist()

    # training and predicting 
    clf = svm.SVC()       # defaut kernal function rbf 
    clf.fit(train_feature, train_label) 
    dump(clf, 'building_classifier.joblib')   # save model to reuse
    print(" --- model trained and saved ---")

    Y_pred = clf.predict(test_feature)  
    # calculate accuracy:
    given_label = test['type'].tolist()
    kappa_value = cohen_kappa_score(given_label, Y_pred)
    print("Kappa value is: ", kappa_value)
    accuracy = len([idx for idx, elem in enumerate(Y_pred) if elem == given_label[idx]])/len(Y_pred)
    print ("Classification accuracy: ", accuracy)

    # export predicted type labal
    test['pred_label'] = Y_pred
    # merge data into one dataframe
    labeled_building = pd.concat([train, test], axis=0, ignore_index=True)
    # can't export geodataframe if it contains two geometry columns
    building_out = labeled_building[['funktion','area', 'type','pred_label','geometry']]
    building_out.to_file(os.path.join(sys.path[0],"data", out_name + ".geojson"))


#%%
building_classifier("predict_Nord-Rhein-Westfalen")


#%%
def building_prediction(model_file, input_file, output_name):

    buildings = gpd.read_file(os.path.join(sys.path[0],"data", input_file), encoding='utf-8')
    buildings = buildings.to_crs({'init': 'epsg:3857'})

    buildings['area'] = buildings['geometry'].area
    # geometry have polygon and linestring, remove the linestring geometry
    buildings = buildings[buildings.area != 0]
    buildings['perimeter'] = buildings['geometry'].length
    buildings['compactness'] = (buildings['area']*4*np.pi)/buildings['perimeter']**2    # compactness
    buildings['convex'] = buildings['geometry'].convex_hull
    buildings['peri_convex'] = buildings['convex'].length
    buildings['area_convex'] = buildings['convex'].area
    buildings['convexity'] = buildings['peri_convex']/buildings['perimeter']    # convexity
    buildings['solidality'] = buildings['area']/buildings['area_convex']     # solidality
    buildings['roundness'] = 4*np.pi*buildings['area']/buildings['peri_convex']**2    # roundness
    print('--1--')
    # load model:
    clf = load(model_file)
    test_feature = np.array(buildings[["area","compactness","convexity","solidality", "roundness"]]).tolist()
    print('--2--')
    type_pred = clf.predict(test_feature) 
    buildings['pred_label'] = type_pred

    buildings = buildings[['gid', 'ags', 'area','pred_label','geometry']]
    buildings.to_file(os.path.join(sys.path[0],"data", output_name + ".gpkg"))
    # print ('--- finished ---')

#%%
building_prediction("building_classifier.joblib", "germany_buildings.zip", 'bayern_builidngs_labeled')

