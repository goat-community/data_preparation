# collection of all OSM tags grouped by features

OSM_tags = {

    'aerialway': ['cable_car', 'gondola', 'mixed_lift', 'chair_lift', 'drag_lift', 't-bar', 'j-bar',
                  'platter', 'rope_tow', 'magic_carpet', 'zip_line', 'goods', 'pylon', 'station'],

    'aeroway':  ['aerodrome', 'apron', 'gate', 'hangar', 'helipad', 'heliport', 'navigationaid', 'runway',
                 'spaceport', 'taxiway', 'terminal', 'windsock', 'stopway', 'holding_position', 'arresting_gear', 'parking_position'],

    'amenity':  ['bar', 'biergarten', 'cafe', 'fast_food', 'food_court', 'ice_cream', 'pub', 'restaurant',
                 'college', 'driving_school', 'kindergarten', 'language_school', 'library', 'toy_library', 'music_school',
                 'school', 'university', 'kick-scooter_rental', 'bicycle_parking', 'bicycle_repair_station', 'bicycle_rental',
                 'boat_rental', 'boat_sharing', 'bus_station', 'car_rental', 'car_sharing', 'car_wash', 'vehicle_inspection',
                 'charging_station', 'ferry_terminal', 'fuel', 'grit_bin', 'motorcycle_parking', 'parking', 'parking_entrance',
                 'parking_space', 'taxi', 'atm', 'bank', 'bureau_de_change', 'baby_hatch', 'clinic', 'dentist', 'doctors',
                 'hospital', 'nursing_home', 'pharmacy', 'social_facility', 'veterinary', 'arts_centre', 'brothel', 'casino',
                 'cinema', 'community_centre', 'conference_centre', 'events_venue', 'fountain', 'gambling', 'love_hotel',
                 'nightclub', 'planetarium', 'public_bookcase', 'social_centre', 'stripclub', 'studio', 'swingerclub',
                 'theatre', 'courthouse', 'fire_station', 'police', 'post_box', 'post_depot', 'post_office', 'prison',
                 'ranger_station', 'townhall', 'bbq', 'bench', 'dog_toilet', 'drinking_water', 'give_box', 'freeshop',
                 'shelter', 'shower', 'telephone', 'toilets', 'water_point', 'watering_place', 'sanitary_dump_station',
                 'recycling', 'waste_basket', 'waste_disposal', 'waste_transfer_station', 'animal_boarding', 'animal_breeding',
                 'animal_shelter', 'baking_oven', 'childcare', 'clock', 'crematorium', 'dive_centre', 'funeral_hall',
                 'grave_yard', 'hunting_stand', 'internet_cafe', 'kitchen', 'kneipp_water_cure', 'lounger', 'marketplace',
                 'monastery', 'photo_booth', 'place_of_mourning', 'place_of_worship', 'public_bath', 'refugee_site',
                 'vending_machine', 'user defined'],

    'barrier':  ['cable_barrier', 'city_wall', 'ditch', 'fence', 'guard_rail', 'handrail', 'hedge', 'kerb',
                 'retaining_wall', 'wall', 'block', 'bollard', 'border_control', 'bump_gate', 'bus_trap', 'cattle_grid',
                 'chain', 'cycle_barrier', 'debris', 'entrance', 'full-height_turnstile', 'gate', 'hampshire_gate',
                 'height_restrictor', 'horse_stile', 'jersey_barrier', 'kissing_gate', 'lift_gate', 'log', 'motorcycle_barrier',
                 'rope', 'sally_port', 'spikes', 'stile', 'sump_buster', 'swing_gate', 'toll_booth', 'turnstile', 'yes'],

    'boundary':  ['aboriginal_lands', 'administrative', 'hazard', 'maritime', 'marker', 'national_park',
                  'political', 'postal_code', 'protected_area', 'special_economic_zone', 'user defined'],

    'building':  ['apartments', 'bungalow', 'cabin', 'detached', 'dormitory', 'farm', 'ger', 'hotel', 'house',
                  'houseboat', 'residential', 'semidetached_house', 'static_caravan', 'terrace', 'commercial', 'industrial',
                  'kiosk', 'office', 'retail', 'supermarket', 'warehouse', 'cathedral', 'chapel', 'church', 'monastery',
                  'mosque', 'presbytery', 'religious', 'shrine', 'synagogue', 'temple', 'bakehouse', 'civic', 'fire_station',
                  'government', 'hospital', 'public', 'toilets', 'train_station', 'transportation', 'kindergarten', 'school',
                  'university', 'college', 'barn', 'conservatory', 'cowshed', 'farm_auxiliary', 'greenhouse', 'slurry_tank',
                  'stable', 'sty', 'grandstand', 'pavilion', 'riding_hall', 'sports_hall', 'stadium', 'hangar', 'hut', 'shed',
                  'carport', 'garage', 'garages', 'parking', 'digester', 'service', 'transformer_tower', 'water_tower',
                  'military', 'bunker', 'bridge', 'construction', 'container', 'tent', 'gatehouse', 'roof', 'ruins',
                  'tree_house', 'yes', 'user defined'],

    'craft':  ['agricultural_engines', 'atelier', 'bakery', 'basket_maker', 'beekeeper', 'blacksmith',
               'boatbuilder', 'bookbinder', 'brewery', 'builder', 'cabinet_maker', 'car_painter', 'carpenter', 'carpet_layer',
               'caterer', 'chimney_sweeper', 'cleaning', 'clockmaker', 'confectionery', 'cooper', 'dental_technician',
               'distillery', 'door_construction', 'dressmaker', 'electronics_repair', 'embroiderer', 'electrician',
               'engraver', 'floorer', 'gardener', 'glaziery', 'goldsmith', 'grinding_mill', 'handicraft', 'hvac',
               'insulation', 'interior_work', 'jeweller', 'joiner', 'key_cutter', 'locksmith', 'metal_construction', 'mint',
               'musical_instrument', 'oil_mill', 'optician', 'organ_builder', 'painter', 'parquet_layer', 'paver',
               'photographer', 'photographic_laboratory', 'piano_tuner', 'plasterer', 'plumber', 'pottery', 'printer',
               'printmaker', 'rigger', 'roofer', 'saddler', 'sailmaker', 'sawmill', 'scaffolder', 'sculptor', 'shoemaker',
               'signmaker', 'stand_builder', 'stonemason', 'stove_fitter', 'sun_protection', 'tailor', 'tiler', 'tinsmith',
               'toolmaker', 'turner', 'upholsterer', 'watchmaker', 'water_well_drilling', 'window_construction', 'winery'],

    'emergency':  ['ambulance_station', 'defibrillator', 'landing_site', 'emergency_ward_entrance',
                   'dry_riser_inlet', 'fire_alarm_box', 'fire_extinguisher', 'fire_hose', 'fire_hydrant', 'water_tank',
                   'suction_point', 'lifeguard', 'lifeguard_base', 'lifeguard_tower', 'lifeguard_platform', 'life_ring',
                   'assembly_point', 'phone', 'siren', 'drinking_water'],

    'geological':  ['moraine', 'outcrop', 'palaeontological_site', 'volcanic_caldera_rim', 'volcanic_vent',
                    'volcanic_lava_field'],

    'highway':  ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential',
                 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'service',
                 'pedestrian', 'track', 'bus_guideway', 'escape', 'raceway', 'road', 'busway', 'footway', 'bridleway', 'steps',
                 'corridor', 'path', 'cycleway', 'proposed', 'construction', 'bus_stop', 'crossing', 'elevator', 'emergency_bay',
                 'emergency_access_point', 'give_way', 'phone', 'milestone', 'mini_roundabout', 'motorway_junction', 'passing_place',
                 'platform', 'rest_area', 'speed_camera', 'street_lamp', 'services', 'stop', 'traffic_mirror', 'traffic_signals',
                 'trailhead', 'turning_circle', 'turning_loop', 'toll_gantry'],

    'healthcare': ['alternative', 'audiologist', 'birthing_center', 'blood_bank', 'blood_donation', 'counselling',
                   'dialysis', 'hospice', 'laboratory', 'midwife', 'nurse', 'occupational_therapist', 'optometrist', 'physiotherapist',
                   'podiatrist', 'psychotherapist', 'rehabilitation', 'sample_collection', 'speech_therapist', 'vaccination_centre'],

    'cycleway':  ['lane', 'opposite', 'opposite_lane', 'track', 'opposite_track', 'share_busway',
                  'opposite_share_busway', 'shared_lane', 'lane'],

    'sidewalk':  ['both', 'left', 'right', 'no'],

    'footway':  ['sidewalk', 'crossing'],

    'historic':  ['aircraft', 'aqueduct', 'archaeological_site', 'battlefield', '	bomb_crater', 'boundary_stone',
                  'building', 'cannon', 'castle', 'castle_wall', 'charcoal_pile', 'church', 'city_gate', 'citywalls', 'farm',
                  'fort', 'gallows', 'highwater_mark', 'locomotive', 'manor', 'memorial', 'milestone', 'monastery', 'monument',
                  'optical_telegraph', 'pa', 'pillory', 'railway_car', 'ruins', 'rune_stone', 'ship', 'tank', 'tomb', 'tower',
                  'vehicle', 'wayside_cross', 'wayside_shrine', 'wreck', 'yes'],

    'landuse': ['commercial', 'construction', 'education', 'industrial', 'residential', 'retail', 'allotments',
                'farmland', 'farmyard', 'flowerbed', 'forest', 'meadow', 'orchard', 'vineyard', 'aquaculture', 'basin',
                'reservoir', 'salt_pond', 'brownfield', 'cemetery', 'depot', 'garages', 'grass', 'greenfield',
                'greenhouse_horticulture', 'landfill', 'military', 'plant_nursery', 'port', 'quarry', 'railway',
                'recreation_ground', 'religious', 'village_green', 'winter_sports', 'user defined',
                'water', 'fallow', 'pasture', 'plantation', 'green_area', 'leisure', 'churchyard', 'highway', 'garden',
                'national_park', 'nature_reserve', 'park', 'grave_yard', 'waters'],

    'leisure':  ['adult_gaming_centre', 'amusement_arcade', 'beach_resort', 'bandstand', 'bird_hide',
                 'common', 'dance', 'disc_golf_course', 'dog_park', 'escape_game', 'firepit', 'fishing', 'fitness_centre',
                 'fitness_station', 'garden', 'hackerspace', 'horse_riding', 'ice_rink', 'marina', 'miniature_golf',
                 'nature_reserve', 'park', 'picnic_table', 'pitch', 'playground', 'slipway', 'sports_centre', 'stadium',
                 'summer_camp', 'swimming_area', 'swimming_pool', 'track', 'water_park'],

    'man_made':  ['adit', 'beacon', 'breakwater', 'bridge', 'bunker_silo', 'carpet_hanger', 'chimney',
                  'communications_tower', 'crane', 'cross', 'cutline', 'clearcut', 'dovecote', 'dyke', 'embankment', 'flagpole',
                  'gasometer', 'goods_conveyor', 'groyne', 'guard_stone', 'kiln', 'lighthouse', 'mast', 'mineshaft',
                  'monitoring_station', 'obelisk', 'observatory', 'offshore_platform', 'petroleum_well', 'pier', 'pipeline',
                  'pump', 'pumping_station', 'reservoir_covered', 'silo', 'snow_fence', 'snow_net', 'storage_tank',
                  'street_cabinet', 'stupa', 'surveillance', 'survey_point', 'tailings_pond', 'telescope', 'tower',
                  'video_wall', 'wastewater_plant', 'watermill', 'water_tower', 'water_well', 'water_tap', 'water_works',
                  'wildlife_crossing', 'windmill', 'works', 'yes'],

    'military':  ['airfield', 'base', 'bunker', 'barracks', 'checkpoint', 'danger_area', 'naval_base',
                  'nuclear_explosion_site', 'obstacle_course', 'office', 'range', 'training_area', 'trench'],

    'natural':  ['wood', 'tree_row', 'tree', 'scrub', 'heath', 'moor', 'grassland', 'fell', 'bare_rock',
                 'scree', 'shingle', 'sand', 'mud', 'water', 'wetland', 'glacier', 'bay', 'strait', 'cape', 'beach',
                 'coastline', 'reef', 'spring', 'hot_spring', 'geyser', 'blowhole', 'peak', 'volcano', 'valley', 'peninsula',
                 'isthmus', 'ridge', 'arete', 'cliff', 'saddle', 'dune', 'rock', 'stone', 'sinkhole', 'cave_entrance'],

    'office':  ['accountant', 'advertising_agency', 'architect', 'association', 'charity', 'company',
                'consulting', 'courier', 'coworking', 'diplomatic', 'educational_institution', 'employment_agency',
                'energy_supplier', 'engineer', 'estate_agent', 'financial', 'financial_advisor', 'forestry', 'foundation',
                'geodesist', 'government', 'graphic_design', 'guide', 'harbour_master', 'insurance', 'it', 'lawyer',
                'logistics', 'moving_company', 'newspaper', 'ngo', 'notary', 'political_party', 'property_management',
                'quango', 'religion', 'research', 'security', 'surveyor', 'tax_advisor', 'telecommunication', 'travel_agent',
                'union', 'visa', 'water_utility', 'yes'],

    'place':  ['country', 'state', 'region', 'province', 'district', 'county', 'municipality', 'city', 'borough',
               'suburb', 'quarter', 'neighbourhood', 'city_block', 'plot', 'town', 'village', 'hamlet', 'isolated_dwelling',
               'farm', 'allotments', 'continent', 'archipelago', 'island', 'islet', 'square', 'locality', 'sea', 'ocean'],

    'power':  ['cable', 'catenary_mast', 'compensator', 'converter', 'generator', 'heliostat', 'insulator',
               'line', 'busbar', 'bay', 'minor_line', 'plant', 'pole', 'portal', 'substation', 'switch', 'switchgear',
               'terminal', 'tower', 'transformer'],

    'public_transport':  ['stop_position', 'platform', 'station', 'stop_area'],

    'railway':  ['abandoned', 'construction', 'disused', 'funicular', 'light_rail', 'miniature', 'monorail',
                 'narrow_gauge', 'preserved', 'rail', 'subway', 'tram', 'halt', 'platform', 'station', 'subway_entrance',
                 'tram_stop', 'buffer_stop', 'derail', 'crossing', 'level_crossing', 'signal', "stop",
                 'switch', 'railway_crossing', 'turntable', 'roundhouse', 'traverser', 'wash', 'user defined'],

    'route':  ['bicycle', 'bus', 'canoe', 'detour', 'ferry', 'foot', 'hiking', 'horse', 'inline_skates',
               'light_rail', 'mtb', 'piste', 'railway', 'road', 'running', 'ski', 'subway', 'train', 'tracks', 'tram',
               'trolleybus'],

    'shop':  ['alcohol', 'bakery', 'beverages', 'brewing_supplies', 'butcher', 'cheese', 'chocolate', 'coffee',
              'confectionery', 'convenience', 'deli', 'dairy', 'farm', 'frozen_food', 'greengrocer', 'health_food',
              'ice_cream', 'pasta', 'pastry', 'seafood', 'spices', 'tea', 'water', 'department_store', 'general', 'kiosk',
              'mall', 'supermarket', 'wholesale', 'baby_goods', 'bag', 'boutique', 'clothes', 'fabric', 'fashion',
              'fashion_accessories', 'jewelry', 'leather', 'sewing', 'shoes', 'tailor', 'watches', 'wool', 'charity',
              'second_hand', 'variety_store', 'beauty', 'chemist', 'cosmetics', 'drugstore', 'erotic', 'hairdresser',
              'hairdresser_supply', 'hearing_aids', 'herbalist', 'massage', 'medical_supply', 'nutrition_supplements',
              'optician', 'perfumery', 'tattoo', 'agrarian', 'appliance', 'bathroom_furnishing', 'doityourself',
              'electrical', 'energy', 'fireplace', 'florist', 'garden_centre', 'garden_furniture', 'gas', 'glaziery',
              'groundskeeping', 'hardware', 'houseware', 'locksmith', 'paint', 'security', 'trade', 'windows', 'antiques',
              'bed', 'candles', 'carpet', 'curtain', 'doors', 'flooring', 'furniture', 'household_linen',
              'interior_decoration', 'kitchen', 'lamps', 'lighting', 'tiles', 'window_blind', 'computer', 'electronics',
              'hifi', 'mobile_phone', 'radiotechnics', 'vacuum_cleaner', 'atv', 'bicycle', 'boat', 'car', 'car_repair',
              'car_parts', 'caravan', 'fuel', 'fishing', 'golf', 'hunting', 'jetski', 'military_surplus', 'motorcycle',
              'outdoor', 'scuba_diving', 'ski', 'snowmobile', 'sports', 'swimming_pool', 'trailer', 'tyres', 'art',
              'collector', 'craft', 'frame', 'games', 'model', 'music', 'musical_instrument', 'photo', 'camera', 'trophy',
              'video', 'video_games', 'anime', 'books', 'gift', 'lottery', 'newsagent', 'stationery', 'ticket', 'bookmaker',
              'cannabis', 'copyshop', 'dry_cleaning', 'e-cigarette', 'funeral_directors', 'laundry', 'money_lender',
              'party', 'pawnbroker', 'pet', 'pet_grooming', 'pest_control', 'pyrotechnics', 'religion', 'storage_rental',
              'tobacco', 'toys', 'travel_agency', 'vacant', 'weapons', 'outpost'],

    'sport':  ['9pin', '10pin', 'american_football', 'aikido', 'archery', 'athletics', 'australian_football',
               'badminton', 'bandy', 'baseball', 'basketball', 'beachvolleyball', 'biathlon', 'billiards', 'bmx',
               'bobsleigh', 'boules', 'bowls', 'boxing', 'bullfighting', 'canadian_football', 'canoe', 'chess', 'cliff_diving',
               'climbing', 'climbing_adventure', 'cockfighting', 'cricket', 'crossfit', 'croquet', 'curling', 'cycle_polo',
               'cycling', 'darts', 'dog_agility', 'dog_racing', 'equestrian', 'fencing', 'field_hockey', 'fitness',
               'five-a-side', 'floorball', 'free_flying', 'futsal', 'gaelic_games', 'golf', 'gymnastics', 'handball',
               'hapkido', 'horseshoes', 'horse_racing', 'ice_hockey', 'ice_skating', 'ice_stock', 'jiu-jitsu', 'judo',
               'karate', 'karting', 'kickboxing', 'kitesurfing', 'korfball', 'krachtbal', 'lacrosse', 'martial_arts',
               'miniature_golf', 'model_aerodrome', 'motocross', 'motor', 'multi', 'netball', 'obstacle_course',
               'orienteering', 'paddle_tennis', 'padel', 'parachuting', 'parkour', 'pedal_car_racing', 'pelota', 'pesäpallo',
               'pickleball', 'pilates', 'pole_dance', 'racquet', 'rc_car', 'roller_skating', 'rowing', 'rugby_league',
               'rugby_union', 'running', 'sailing', 'scuba_diving', 'shooting', 'shot-put', 'skateboard', 'ski_jumping',
               'skiing', 'snooker', 'soccer', 'speedway', 'squash', 'sumo', 'surfing', 'swimming', 'table_tennis',
               'table_soccer', 'taekwondo', 'tennis', 'toboggan', 'ultimate', 'volleyball', 'wakeboarding', 'water_polo',
               'water_ski', 'weightlifting', 'wrestling', 'yoga', 'zurkhaneh_sport'],

    'telecom':  ['exchange', 'connection_point', 'distribution_point', 'service_device', 'data_center'],

    'tourism':  ['alpine_hut', 'apartment', 'aquarium', 'artwork', 'attraction', 'camp_pitch', 'camp_site',
                 'caravan_site', 'chalet', 'gallery', 'guest_house', 'hostel', 'hotel', 'information', 'motel', 'museum',
                 'picnic_site', 'theme_park', 'viewpoint', 'wilderness_hut', 'zoo', 'yes'],

    'water':  ['river', 'oxbow', 'canal', 'ditch', 'lock', 'fish_pass', 'lake', 'reservoir', 'pond', 'basin',
               'lagoon', 'stream_pool', 'reflecting_pool', 'moat', 'wastewater'],

    'waterway':  ['river', 'riverbank', 'stream', 'tidal_channel', 'canal', 'drain', 'ditch', 'pressurised',
                  'fairway', 'dock', 'boatyard', 'dam', 'weir', 'waterfall', 'lock_gate', 'soakhole', 'turning_point',
                  'water_point', 'fuel']
}

OSM_germany = {
    'regions':             ["brandenburg", "bremen", "hamburg", "hessen","mecklenburg-vorpommern",
                            "rheinland-pfalz", "saarland",  "sachsen", "sachsen-anhalt","niedersachsen", 
                            "schleswig-holstein", "thueringen"],
    'baden-wuerttemberg' : ['freiburg-regbez', 'karlsruhe-regbez', 'stuttgart-regbez', 'tuebingen-regbez'],
    'bayern' :             ["mittelfranken", "niederbayern", "oberbayern", "oberfranken", "oberpfalz", "schwaben", "unterfranken"],
    'nordrhein-westfalen': ["arnsberg-regbez", "detmold-regbez", "duesseldorf-regbez", "koeln-regbez", "muenster-regbez"],
}

gms_keys = {
    "01" : "schleswig_holstein", "02" : "hamburg", "03" : "niedersachsen", "04" : "bremen", "051" : "duesseldorf", "053" : "koeln",
    "055" : "muenster", "057" : "arnsberg", "06" : "hessen", "07" : "rheinland_pfalz", "081" : "stuttgart", "082" : "karlsruhe", 
    "083" : "freiburg", "084" : "tuebingen", "091" : "oberbayern", "092" : "niederbayern", "093" : "oberpfalz", "094" : "oberfranken", 
    "095" : "mittelfranken", "096" : "unterfranken", "097" : "schwaben", "10" : "saarland", "12" : "brandenburg", "13" : "mecklenburg_vorpommern", 
    "14" : "sachsen", "15" : "sachsen-anhalt", "16" : "thueringen"
}
    
