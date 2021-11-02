# collection of all OSM tags grouped by features

OSM_tags = {

    'aerialway' : ['cable_car', 'gondola', 'mixed_lift', 'chair_lift', 'drag_lift', 't-bar', 'j-bar',
    'platter', 'rope_tow', 'magic_carpet', 'zip_line', 'goods', 'pylon', 'station'],

    'aeroway' :  ['aerodrome', 'apron', 'gate', 'hangar', 'helipad', 'heliport', 'navigationaid', 'runway', 
    'spaceport', 'taxiway', 'terminal', 'windsock', 'stopway', 'holding_position', 'arresting_gear', 'parking_position'],
    
    'amenity' :  ['bar', 'biergarten', 'cafe', 'fast_food', 'food_court', 'ice_cream', 'pub', 'restaurant', 
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

    'barrier' :  ['cable_barrier', 'city_wall', 'ditch', 'fence', 'guard_rail', 'handrail', 'hedge', 'kerb', 
    'retaining_wall', 'wall', 'block', 'bollard', 'border_control', 'bump_gate', 'bus_trap', 'cattle_grid', 
    'chain', 'cycle_barrier', 'debris', 'entrance', 'full-height_turnstile', 'gate', 'hampshire_gate', 
    'height_restrictor', 'horse_stile', 'jersey_barrier', 'kissing_gate', 'lift_gate', 'log', 'motorcycle_barrier', 
    'rope', 'sally_port', 'spikes', 'stile', 'sump_buster', 'swing_gate', 'toll_booth', 'turnstile', 'yes'],

    'boundary' :  ['aboriginal_lands', 'administrative', 'hazard', 'maritime', 'marker', 'national_park', 
    'political', 'postal_code', 'protected_area', 'special_economic_zone', 'user defined'],

    'building' :  ['apartments', 'bungalow', 'cabin', 'detached', 'dormitory', 'farm', 'ger', 'hotel', 'house', 
    'houseboat', 'residential', 'semidetached_house', 'static_caravan', 'terrace', 'commercial', 'industrial', 
    'kiosk', 'office', 'retail', 'supermarket', 'warehouse', 'cathedral', 'chapel', 'church', 'monastery', 
    'mosque', 'presbytery', 'religious', 'shrine', 'synagogue', 'temple', 'bakehouse', 'civic', 'fire_station', 
    'government', 'hospital', 'public', 'toilets', 'train_station', 'transportation', 'kindergarten', 'school', 
    'university', 'college', 'barn', 'conservatory', 'cowshed', 'farm_auxiliary', 'greenhouse', 'slurry_tank', 
    'stable', 'sty', 'grandstand', 'pavilion', 'riding_hall', 'sports_hall', 'stadium', 'hangar', 'hut', 'shed', 
    'carport', 'garage', 'garages', 'parking', 'digester', 'service', 'transformer_tower', 'water_tower', 
    'military', 'bunker', 'bridge', 'construction', 'container', 'tent', 'gatehouse', 'roof', 'ruins', 
    'tree_house', 'yes', 'user defined'],

    'craft' :  ['agricultural_engines', 'atelier', 'bakery', 'basket_maker', 'beekeeper', 'blacksmith', 
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

    'emergency' :  ['ambulance_station', 'defibrillator', 'landing_site', 'emergency_ward_entrance', 
    'dry_riser_inlet', 'fire_alarm_box', 'fire_extinguisher', 'fire_hose', 'fire_hydrant', 'water_tank', 
    'suction_point', 'lifeguard', 'lifeguard_base', 'lifeguard_tower', 'lifeguard_platform', 'life_ring', 
    'assembly_point', 'phone', 'siren', 'drinking_water'],

    'geological' :  ['moraine', 'outcrop', 'palaeontological_site', 'volcanic_caldera_rim', 'volcanic_vent', 
    'volcanic_lava_field'],

    'highway' :  ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential', 
    'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'service', 
    'pedestrian', 'track', 'bus_guideway', 'escape', 'raceway', 'road', 'busway', 'footway', 'bridleway', 'steps', 
    'corridor', 'path', 'cycleway', 'proposed', 'construction', 'bus_stop', 'crossing', 'elevator', 'emergency_bay', 
    'emergency_access_point', 'give_way', 'phone', 'milestone', 'mini_roundabout', 'motorway_junction', 'passing_place', 
    'platform', 'rest_area', 'speed_camera', 'street_lamp', 'services', 'stop', 'traffic_mirror', 'traffic_signals', 
    'trailhead', 'turning_circle', 'turning_loop', 'toll_gantry'],

    'healthcare' : ['alternative', 'audiologist', 'birthing_center', 'blood_bank', 'blood_donation', 'counselling',
    'dialysis', 'hospice', 'laboratory', 'midwife', 'nurse', 'occupational_therapist', 'optometrist', 'physiotherapist',
    'podiatrist', 'psychotherapist', 'rehabilitation', 'sample_collection', 'speech_therapist', 'vaccination_centre'],

    'cycleway' :  ['lane', 'opposite', 'opposite_lane', 'track', 'opposite_track', 'share_busway', 
    'opposite_share_busway', 'shared_lane', 'lane'],

    'sidewalk' :  ['both', 'left', 'right', 'no'],

    'footway' :  ['sidewalk', 'crossing'],

    'historic' :  ['aircraft', 'aqueduct', 'archaeological_site', 'battlefield', '	bomb_crater', 'boundary_stone', 
    'building', 'cannon', 'castle', 'castle_wall', 'charcoal_pile', 'church', 'city_gate', 'citywalls', 'farm', 
    'fort', 'gallows', 'highwater_mark', 'locomotive', 'manor', 'memorial', 'milestone', 'monastery', 'monument', 
    'optical_telegraph', 'pa', 'pillory', 'railway_car', 'ruins', 'rune_stone', 'ship', 'tank', 'tomb', 'tower', 
    'vehicle', 'wayside_cross', 'wayside_shrine', 'wreck', 'yes'],

    'landuse' : ['commercial', 'construction', 'education', 'industrial', 'residential', 'retail', 'allotments', 
    'farmland', 'farmyard', 'flowerbed', 'forest', 'meadow', 'orchard', 'vineyard', 'aquaculture', 'basin', 
    'reservoir', 'salt_pond', 'brownfield', 'cemetery', 'depot', 'garages', 'grass', 'greenfield', 
    'greenhouse_horticulture', 'landfill', 'military', 'plant_nursery', 'port', 'quarry', 'railway', 
    'recreation_ground', 'religious', 'village_green', 'winter_sports', 'user defined', 
    'water', 'fallow', 'pasture', 'plantation', 'green_area', 'leisure', 'churchyard', 'highway', 'garden',
    'national_park', 'nature_reserve', 'park', 'grave_yard'],

    'leisure' :  ['adult_gaming_centre', 'amusement_arcade', 'beach_resort', 'bandstand', 'bird_hide', 
    'common', 'dance', 'disc_golf_course', 'dog_park', 'escape_game', 'firepit', 'fishing', 'fitness_centre', 
    'fitness_station', 'garden', 'hackerspace', 'horse_riding', 'ice_rink', 'marina', 'miniature_golf', 
    'nature_reserve', 'park', 'picnic_table', 'pitch', 'playground', 'slipway', 'sports_centre', 'stadium', 
    'summer_camp', 'swimming_area', 'swimming_pool', 'track', 'water_park'],

    'man_made' :  ['adit', 'beacon', 'breakwater', 'bridge', 'bunker_silo', 'carpet_hanger', 'chimney', 
    'communications_tower', 'crane', 'cross', 'cutline', 'clearcut', 'dovecote', 'dyke', 'embankment', 'flagpole', 
    'gasometer', 'goods_conveyor', 'groyne', 'guard_stone', 'kiln', 'lighthouse', 'mast', 'mineshaft', 
    'monitoring_station', 'obelisk', 'observatory', 'offshore_platform', 'petroleum_well', 'pier', 'pipeline', 
    'pump', 'pumping_station', 'reservoir_covered', 'silo', 'snow_fence', 'snow_net', 'storage_tank', 
    'street_cabinet', 'stupa', 'surveillance', 'survey_point', 'tailings_pond', 'telescope', 'tower', 
    'video_wall', 'wastewater_plant', 'watermill', 'water_tower', 'water_well', 'water_tap', 'water_works', 
    'wildlife_crossing', 'windmill', 'works', 'yes'],

    'military' :  ['airfield','base' ,'bunker', 'barracks', 'checkpoint', 'danger_area', 'naval_base', 
    'nuclear_explosion_site', 'obstacle_course', 'office', 'range', 'training_area', 'trench'],

    'natural' :  ['wood', 'tree_row', 'tree', 'scrub', 'heath', 'moor', 'grassland', 'fell', 'bare_rock', 
    'scree', 'shingle', 'sand', 'mud', 'water', 'wetland', 'glacier', 'bay', 'strait', 'cape', 'beach', 
    'coastline', 'reef', 'spring', 'hot_spring', 'geyser', 'blowhole', 'peak', 'volcano', 'valley', 'peninsula', 
    'isthmus', 'ridge', 'arete', 'cliff', 'saddle', 'dune', 'rock', 'stone', 'sinkhole', 'cave_entrance'],

    'office' :  ['accountant', 'advertising_agency', 'architect', 'association', 'charity', 'company', 
    'consulting', 'courier', 'coworking', 'diplomatic', 'educational_institution', 'employment_agency', 
    'energy_supplier', 'engineer', 'estate_agent', 'financial', 'financial_advisor', 'forestry', 'foundation', 
    'geodesist', 'government', 'graphic_design', 'guide', 'harbour_master', 'insurance', 'it', 'lawyer', 
    'logistics', 'moving_company', 'newspaper', 'ngo', 'notary', 'political_party', 'property_management', 
    'quango', 'religion', 'research', 'security', 'surveyor', 'tax_advisor', 'telecommunication', 'travel_agent', 
    'union', 'visa', 'water_utility', 'yes'],

    'place' :  ['country', 'state', 'region', 'province', 'district', 'county', 'municipality', 'city', 'borough', 
    'suburb', 'quarter', 'neighbourhood', 'city_block', 'plot', 'town', 'village', 'hamlet', 'isolated_dwelling', 
    'farm', 'allotments', 'continent', 'archipelago', 'island', 'islet', 'square', 'locality', 'sea', 'ocean'],

    'power' :  ['cable', 'catenary_mast', 'compensator', 'converter', 'generator', 'heliostat', 'insulator', 
    'line', 'busbar', 'bay', 'minor_line', 'plant', 'pole', 'portal', 'substation', 'switch', 'switchgear', 
    'terminal', 'tower', 'transformer'],

    'public_transport' :  ['stop_position', 'platform', 'station', 'stop_area'],

    'railway' :  ['abandoned', 'construction', 'disused', 'funicular', 'light_rail', 'miniature', 'monorail', 
    'narrow_gauge', 'preserved', 'rail', 'subway', 'tram', 'halt', 'platform', 'station', 'subway_entrance', 
    'tram_stop', 'buffer_stop', 'derail', 'crossing', 'level_crossing', 'signal', "stop",
    'switch', 'railway_crossing', 'turntable', 'roundhouse', 'traverser', 'wash', 'user defined'],

    'route' :  ['bicycle', 'bus', 'canoe', 'detour', 'ferry', 'foot', 'hiking', 'horse', 'inline_skates', 
    'light_rail', 'mtb', 'piste', 'railway', 'road', 'running', 'ski', 'subway', 'train', 'tracks', 'tram', 
    'trolleybus'],

    'shop' :  ['alcohol', 'bakery', 'beverages', 'brewing_supplies', 'butcher', 'cheese', 'chocolate', 'coffee', 
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

    'sport' :  ['9pin', '10pin', 'american_football', 'aikido', 'archery', 'athletics', 'australian_football', 
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

    'telecom' :  ['exchange', 'connection_point', 'distribution_point', 'service_device', 'data_center'],

    'tourism' :  ['alpine_hut', 'apartment', 'aquarium', 'artwork', 'attraction', 'camp_pitch', 'camp_site', 
    'caravan_site', 'chalet', 'gallery', 'guest_house', 'hostel', 'hotel', 'information', 'motel', 'museum', 
    'picnic_site', 'theme_park', 'viewpoint', 'wilderness_hut', 'zoo', 'yes'],

    'water' :  ['river', 'oxbow', 'canal', 'ditch', 'lock', 'fish_pass', 'lake', 'reservoir', 'pond', 'basin', 
    'lagoon', 'stream_pool', 'reflecting_pool', 'moat', 'wastewater'],

    'waterway' :  ['river', 'riverbank', 'stream', 'tidal_channel', 'canal', 'drain', 'ditch', 'pressurised', 
    'fairway', 'dock', 'boatyard', 'dam', 'weir', 'waterfall', 'lock_gate', 'soakhole', 'turning_point', 
    'water_point', 'fuel']
}

# tags landuse
landuse_tags_not_used_in_SQL = {

    'landuse' : ['construction -> do not use it', 'education -> community', 'flowerbed -> nature', 'brownfield -> construction', 
                 'depot -> commercial', 'greenfield -> construction?', 'port -> commercial?', 'winter_sports -> leisure', 'user defined']

    }


# sorted as SQL script -> do not use the ones within buildings as amenity is used for the greater area and building for the building itself

landuse_simplified = { 
        'water'            : ['basin (also in water)', 'reservoir (also in man_made and water)', 'salt_pond', 'water'],
        'agriculture'      : ['allotments (also in place)', 'aquaculture', 'farmland', 'farmyard', 'greenhouse_horticulture', 'orchard', 'plant_nursery', 'vineyard'],
        'nature'           : ['forest', 'grass', 'meadow'],
        'leisure'          : ['village_green', 'recreation_ground', 'garden (also in leisure tags)', 'national_park (also in boundary tags)', 
                              'nature_reserve (also in leisure tags)', 'park (also in leisure tags)'],
        'cemetery'         : ['cemetery', 'grave_yard (also in amenity tags)'],
        'residential'      : ['residential (also in buildings and highways)', 'garages (also in buildings)'],
        'commercial'       : ['commercial (also in buildings)', 'retail (also in buildings)'],
        'community'        : ['religious (also in buildings)'],
        'industrial'       : ['industrial (also in buildings)', 'landfill', 'quarry'],
        'transportation'   : ['railway (also own group and in route tags)', 'highway (also own group'],
        'military'         : ['military (also in building)']

}

# Data that are collected from different groups (not landuse) within the SQL script

landuse_tags_from_different_groups_already_in_SQL  = {
        'transportation'   : ['parking from amenity'],
        'community'        : ['school from amenity', 'hospital from amenity'],
        'water'            : ['swimming_pool from leisure', 'all leisure tags except swimming_pool', 'water in natural'],
        'nature'           : ['scrub ,wood ,wetland ,grassland, heath -> all in natural']

}


# fehlt im Vergleich zu SQL
# sollen am Ende alle Tags irgendwo eingordnet sein/ verwendet worden sein?

landuse_tags_from_different_groups_additional = {
        'water'            : ['swimnming_pool in shop', 'water as own group and water in shop'], 
        'agriculture'      : [],
        'nature'           : [],
        'leisure'          : [],
        'cemetery'         : [],
        'residential'      : [],
        'commercial'       : [],
        'community'        : ['school in building tags', 'university in amenity and building tags', 'hospital in building tags' , 'college in amenity and building tags'],
        'industrial'       : [],
        'transportation'   : []

}  
OSM_tags_sorted_for_landuse_classified_or_buildings_or_pois = {

    #
    'aerialway'                 :  ['cable_car', 'gondola', 'mixed_lift', 'chair_lift', 'drag_lift', 't-bar', 'j-bar',
                                    'platter', 'rope_tow', 'magic_carpet', 'zip_line', ('goods'), ('pylon'), 'station'],
                                   
    'aerialway_pois'            :  ['cable_car', 'gondola', 'mixed_lift', 'chair_lift', 'drag_lift', 't-bar', 'j-bar',
                                    'platter', 'rope_tow', 'magic_carpet', 'zip_line', 'station'], #-> later group all ski related ones, but gondolas for PuT?
    'aerialway_landuse'         :  [],
    'aerialway_buildings'       :  ['station'],
    'aerialway_street_network'  :  [],

    #
    'aeroway'                   :  ['aerodrome', 'apron', 'gate', 'hangar', ('helipad'), 'heliport', ('navigationaid'), 'runway', 'spaceport', 'taxiway', 'terminal',  
                                   ('windsock'), 'stopway', ('holding_position'), ('arresting_gear'), 'parking_position'],

    'aeroway_pois'              :  ['aerodrome', 'gate', 'heliport', 'terminal'], # -> probably enough to add the airport itself? 
    'aeroway_landuse'           :  ['aerodrome', 'apron', 'heliport', 'runway', 'spaceport', 'taxiway', 'terminal', 'stopway', 'parking_position'],
    'aeroway_buildings'         :  ['hangar', 'terminal'],
    'aeroway_street_network'    :  [],

    #   
    'amenity'                   :  ['bar', 'biergarten', 'cafe', 'fast_food', ('food_court'), 'ice_cream', 'pub', 'restaurant', 
                                    'college', 'driving_school', 'kindergarten', 'language_school', 'library', 'toy_library', 'music_school', 
                                    'school', 'university', ('kick-scooter_rental'), 'bicycle_parking', 'bicycle_repair_station', ('bicycle_rental'), 
                                    ('boat_rental'), ('boat_sharing'), 'bus_station', ('car_rental'), ('car_sharing'), 'car_wash', 'vehicle_inspection', 
                                    'charging_station', 'ferry_terminal', 'fuel', ('grit_bin'), 'motorcycle_parking', 'parking', ('parking_entrance'), 
                                    'parking_space', 'taxi', 'atm', 'bank', ('bureau_de_change'), ('baby_hatch'), 'clinic', 'dentist', 'doctors', 
                                    'hospital', 'nursing_home', 'pharmacy', 'social_facility', 'veterinary', 'arts_centre', 'brothel', 'casino', 
                                    'cinema', 'community_centre', 'conference_centre', 'events_venue', ('fountain'), ('gambling'), ('love_hotel'), 
                                    'nightclub', ('planetarium'), ('public_bookcase'), 'social_centre', 'stripclub', 'studio', ('swingerclub'), 
                                    'theatre', 'courthouse', 'fire_station', 'police', ('post_box'), 'post_depot', 'post_office', 'prison',
                                    ('ranger_station'), 'townhall', ('bbq'), ('bench'), ('dog_toilet'), ('drinking_water'), ('give_box'), ('freeshop'),
                                    'shelter', ('shower'), ('telephone'), ('toilets'), ('water_point'), ('watering_place'), ('sanitary_dump_station'),
                                    'recycling', ('waste_basket'), ('waste_disposal'), ('waste_transfer_station'), ('animal_boarding'), 'animal_breeding',
                                    ('animal_shelter'), ('baking_oven'), 'childcare', ('clock'), 'crematorium', ('dive_centre'), 'funeral_hall', 
                                    'grave_yard', ('hunting_stand'), ('internet_cafe'), ('kitchen'), ('kneipp_water_cure'), ('lounger'), 'marketplace', 
                                    'monastery', ('photo_booth'), ('place_of_mourning'), 'place_of_worship', ('public_bath'), ('refugee_site'), 
                                    ('vending_machine'), ('user defined')],

# bei baby_hatch gehts weiter
# https://taginfo.openstreetmap.org/keys/amenity
# https://wiki.openstreetmap.org/wiki/Map_features
# https://muenchen.open-accessibility.org/ to check which POIs are used in the final Software

    'amenity_pois'              :  ['bar', 'biergarten', 'cafe', 'fast_food', 'ice_cream', 'pub', 'restaurant', 'library',
                                    ('bicycle_repair_station'),
                                    'bus_station', 
                                    'charging_station',
                                    'taxi', 'atm', 'bank',
                                    'arts_centre',
                                    'cinema', 'community_centre',
                                    'nightclub',
                                    'theatre', 'post_box', 'post_office', 'toilets' '(maybe not due to paywall)',
                                    'recycling',
                                    'marketplace',],
    'amenity_landuse'           :  ['bar', 'biergarten', 'cafe', 'fast_food', 'ice_cream', 'pub', 'restaurant', 
                                    'college', 'driving_school', 'kindergarten', 'language_school', 'library', 'toy_library', 'music_school',
                                    'school', 'university', 'bicycle_parking', 'bicycle_repair_station',
                                    'bus_station', 'car_wash', 'vehicle_inspection',
                                    'ferry_terminal', 'fuel' '(transport or commercial)', 
                                    'motorcycle_parking', 'parking', 
                                    'parking_space', 'clinic', 'dentist', 'doctors',
                                    'hospital', 'nursing_home', 'pharmacy', 'social_facility', 'veterinary','arts_centre' '(community or commercial)', 'brothel', 'casino',
                                    'cinema', 'community_centre', 'conference_centre', 'events_venue',
                                    'nightclub', 'social_centre', 'stripclub', 'studio',
                                    'theatre', 'courthouse' '(which landuse?)', 'fire_station', 'police', 'post_depot', 'post_office', 'prison' '(which landuse)',
                                    'townhall', 'shelter' '(community?)', 
                                    'animal_breeding',
                                    'childcare', 'crematorium', 'funeral_hall', 'grave_yard', 'kneipp_water_cure', 
                                    'monastery', 'place_of_worship',
                                    ],
    'amenity_buildings'         :  ['bar', 'cafe', 'fast_food', 'ice_cream', 'pub', 'restaurant', 
                                    'driving_school', 'language_school', 'library', 'toy_library', 'music_school',
                                    'vehicle_inspection', 
                                    'bank', 'clinic', 'dentist', 'doctors',
                                    'nursing_home', 'pharmacy', 'social_facility', 'veterinary','arts_centre', 'brothel', 'casino',
                                    'cinema', 'community_centre', 'conference_centre', 'events_venue',
                                    'nightclub', 'social_centre', 'stripclub', 'studio',
                                    'theatre','courthouse', 'police', 'post_depot', 'post_office', 
                                    'townhall',
                                    'childcare', 'crematorium', 'funeral_hall' 
                                    ],
    'amenity_street_network'    :  [],
    
    #
    'barrier'                   :  [('cable_barrier'), 'city_wall', 'ditch', 'fence', ('guard_rail'), ('handrail'), ('hedge'), ('kerb'), 
                                    ('retaining_wall'), ('wall'), ('block'), ('bollard'), ('border_control'), ('bump_gate'), ('bus_trap'), ('cattle_grid'), 
                                    ('chain'), ('cycle_barrier'), ('debris'), ('entrance'), 'full-height_turnstile', 'gate', 'hampshire_gate', 
                                    'height_restrictor', 'horse_stile', 'jersey_barrier', 'kissing_gate', 'lift_gate', 'log', 'motorcycle_barrier', 
                                    'rope', 'sally_port', 'spikes', 'stile', 'sump_buster', 'swing_gate', 'toll_booth', 'turnstile', 'yes'],

    'barrier_pois'              :   [],

    'barrier_landuse'           :   [],

    'barrier_buildings'         :   [],

    'barrier_street_network'    :   ['city_wall', 'ditch', 'fence', 
                                     '(stop half way through as most of them only make sense if complete) ' 
                                     ],

    #
    'boundary'                  :  [('aboriginal_lands'), ('administrative'), ('hazard'), ('maritime'), ('marker'), 'national_park', 
                                    ('political'), ('postal_code'), 'protected_area', ('special_economic_zone'), ('user defined')],

    'boundary_pois'             :   [],

    'boundary_landuse'          :   ['national_park', 'protected_area'],

    'boundary_buildings'        :   [],

    'boundary_street_network'   :   [],


    'building'                  :  ['apartments', ('bungalow'), ('cabin'), 'detached', 'dormitory', 'farm', ('ger'), 'hotel', 'house', 
                                    ('houseboat'), 'residential', 'semidetached_house', ('static_caravan'), 'terrace', 'commercial', 'industrial', 
                                    'kiosk', 'office', 'retail', 'supermarket', 'warehouse', 'cathedral', 'chapel', 'church', 'monastery', 
                                    'mosque', 'presbytery', 'religious', 'shrine', 'synagogue', 'temple', ('bakehouse'), 'civic', 'fire_station', 
                                    'government', 'hospital', 'public', 'toilets', 'train_station', 'transportation', 'kindergarten', 'school', 
                                    'university', 'college', 'barn', ('conservatory'), 'cowshed', 'farm_auxiliary', 'greenhouse', 'slurry_tank', 
                                    'stable', 'sty', 'grandstand', 'pavilion', 'riding_hall', 'sports_hall', 'stadium', 'hangar', ('hut'), ('shed'), 
                                    'carport', 'garage', 'garages', 'parking', 'digester', ('service' 'new type needed'), ('transformer_tower'), ('water_tower'), 
                                    'military', 'bunker', 'bridge', 'construction', ('container'), ('tent'), ('gatehouse'), ('roof'), ('ruins'), 
                                    ('tree_house'), ('yes'), ('user defined')],

    'building_pois'             :   ['hotel', 'kiosk', 'supermarket', 'hospital', 'toilets', 'train_station'],

    'building_landuse'          :  ['apartments', 'detached', 'dormitory', 'farm', 'hotel', 'house', 
                                    'residential', 'semidetached_house', 'terrace', 'commercial', 'industrial',
                                    'kiosk', 'office', 'retail', 'supermarket', 'warehouse', 'cathedral', 'chapel', 'church', 'monastery',
                                    'mosque', 'presbytery', 'religious', 'shrine', 'synagogue', 'temple', 'civic', 'fire_station', 
                                    'government', 'hospital', 'public', 'toilets', 'train_station', 'transportation', 'kindergarten', 'school',
                                    'university', 'college', 'barn', 'cowshed', 'farm_auxiliary', 'greenhouse', 'slurry_tank',
                                    'stable', 'sty', 'grandstand', 'pavilion', 'riding_hall', 'sports_hall', 'stadium', 'hangar',
                                    'carport', 'garage', 'garages', 'parking', 'digester',
                                    'military', 'bunker', 'construction'
                                     ],

    'building_buildings'        :  ['apartments', 'detached', 'dormitory', 'farm', 'hotel', 'house', 
                                    'residential', 'semidetached_house', 'terrace', 'commercial', 'industrial',
                                    'kiosk', 'office', 'retail', 'supermarket', 'warehouse', 'cathedral', 'chapel', 'church', 'monastery',
                                    'mosque', 'presbytery', 'religious', 'shrine', 'synagogue', 'temple', 'civic', 'fire_station', 
                                    'government', 'hospital', 'public', 'toilets', 'train_station', 'transportation', 'kindergarten', 'school',
                                    'university', 'college', 'barn', 'cowshed', 'farm_auxiliary', 'greenhouse', 'slurry_tank',
                                    'stable', 'sty', 'grandstand', 'pavilion', 'riding_hall', 'sports_hall', 'stadium', 'hangar',
                                    'carport', 'garage', 'garages', 'parking', 'digester',
                                    'military', 'bunker'
                                    ],

    'building_street_network'   :   ['bridge'],

    #
    'craft'                     :  ['agricultural_engines', 'atelier', 'bakery', 'basket_maker', 'beekeeper', 'blacksmith', 
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

    'craft_pois'                :   [],

    'craft_landuse'             :  ['(all of them)' 'agricultural_engines', 'atelier', 'bakery', 'basket_maker', 'beekeeper', 'blacksmith', 
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

    'craft_buildings'           :  ['(all of them)' 'agricultural_engines', 'atelier', 'bakery', 'basket_maker', 'beekeeper', 'blacksmith', 
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

    'craft_street_network'      :   [],

    #
    'emergency'                 :  ['ambulance_station', ('defibrillator'), ('landing_site'), ('emergency_ward_entrance'),
                                    ('dry_riser_inlet'), ('fire_alarm_box'), ('fire_extinguisher'), ('fire_hose'), ('fire_hydrant'), ('water_tank'), 
                                    ('suction_point'), ('lifeguard'), ('lifeguard_base'), ('lifeguard_tower'), ('lifeguard_platform'), ('life_ring'), 
                                    ('assembly_point'), ('phone'), ('siren'), ('drinking_water')],

    'emergency_pois'            :  [],

    'emergency_landuse'         :  ['ambulance_station'],

    'emergency_buildings'       :  ['ambulance_station'],

    'emergency_street_network'  :  [],

    #
    'geological'                :  ['moraine', 'outcrop', 'palaeontological_site', 'volcanic_caldera_rim', 'volcanic_vent', 
                                    'volcanic_lava_field'],

    'geological_pois'           :  [],

    'geological_landuse'        :  '[nature = green area does not make sense]',

    'geological_buildings'      :  [],

    'geological_street_network' :  [],

    #
    'healthcare'                :  ['alternative', 'audiologist', 'birthing_center', 'blood_bank', 'blood_donation', 'counselling',
                                    'dialysis', 'hospice', 'laboratory', 'midwife', 'nurse', 'occupational_therapist', 'optometrist', 'physiotherapist',
                                    'podiatrist', 'psychotherapist', 'rehabilitation', 'sample_collection', 'speech_therapist', 'vaccination_centre'],

    'healthcare_pois'           :  [],

    'healthcare_landuse'        :  ['(all of them)' 'alternative', 'audiologist', 'birthing_center', 'blood_bank', 'blood_donation', 'counselling',
                                    'dialysis', 'hospice', 'laboratory', 'midwife', 'nurse', 'occupational_therapist', 'optometrist', 'physiotherapist',
                                    'podiatrist', 'psychotherapist', 'rehabilitation', 'sample_collection', 'speech_therapist', 'vaccination_centre'],

    'healthcare_buildings'      :  ['(all of them)' 'alternative', 'audiologist', 'birthing_center', 'blood_bank', 'blood_donation', 'counselling',
                                    'dialysis', 'hospice', 'laboratory', 'midwife', 'nurse', 'occupational_therapist', 'optometrist', 'physiotherapist',
                                    'podiatrist', 'psychotherapist', 'rehabilitation', 'sample_collection', 'speech_therapist', 'vaccination_centre'],

    'healthcare_street_network' :  [],

    #
    'highway'                   :  ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential', 
                                    'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'service', 
                                    'pedestrian', 'track', 'bus_guideway', 'escape', 'raceway', 'road', 'busway', 'footway', 'bridleway', 'steps', 
                                    'corridor', 'path', 'cycleway', ('proposed'), 'construction', 'bus_stop', 'crossing', ('elevator'), 'emergency_bay', 
                                    ('emergency_access_point'), ('give_way'), ('phone'), ('milestone'), 'mini_roundabout', 'motorway_junction', 'passing_place', 
                                    'platform', 'rest_area', ('speed_camera'), ('street_lamp'), 'services', ('stop'), ('traffic_mirror'), ('traffic_signals'), 
                                    ('trailhead'), 'turning_circle', 'turning_loop', ('toll_gantry')],

    'highway_pois'              :  [('bus_stop')],

    'highway_landuse'           :  ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential', 
                                    'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'service', 
                                    'pedestrian', 'track', 'bus_guideway', 'escape', 'raceway', 'road', 'busway', 'footway', 'bridleway', 'steps', 
                                    'corridor', 'path', 'cycleway','construction','bus_stop','emergency_bay', 'motorway_junction', 'passing_place',
                                    'platform', 'rest_area', 'services', 
                                    'turning_circle',],

    'highway_buildings'         :  [],

    'highway_street_network'    :  ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential', 
                                    'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'service', 
                                    'pedestrian', 'track', 'bus_guideway', 'escape', 'raceway', 'road', 'busway', 'footway', 'bridleway', 'steps', 
                                    'corridor', 'path', 'cycleway', 'bus_stop', 'crossing', 'motorway_junction', 'platform', 'turning_circle',],

    #
    'cycleway'                  :  ['lane', 'opposite', 'opposite_lane', 'track', 'opposite_track', 'share_busway', 
                                    'opposite_share_busway', 'shared_lane', 'lane'],

    'cycleway_pois'             :  [],

    'cycleway_landuse'          :  ['lane', 'track'],

    'cycleway_buildings'        :  [],

    'cycleway_street_network'   :  ['lane', 'opposite', 'opposite_lane', 'track', 'opposite_track', 'share_busway', 
                                    'opposite_share_busway', 'shared_lane', 'lane'],

    #
    'sidewalk'                  :  ['both', 'left', 'right', 'no'],
    
    'sidewalk_pois'             :  [],

    'sidewalk_landuse'          :  [],

    'sidewalk_buildings'        :  [],

    'sidewalk_street_network'   :  ['both', 'left', 'right', 'no'],

    #
    'footway'                   :  ['sidewalk', 'crossing'],
        
    'footway_pois'              :  [],

    'footway_landuse'           :  ['sidewalk'],

    'footway_buildings'         :  [],

    'footway_street_network'    :  ['sidewalk', 'crossing'],

    # don't know how to classify that
    'historic'                  :  ['aircraft', 'aqueduct', 'archaeological_site', 'battlefield', '	bomb_crater', ('boundary_stone'), 
                                    'building', ('cannon'), 'castle', 'castle_wall', 'charcoal_pile', 'church', 'city_gate', 'citywalls', 'farm', 
                                    'fort', 'gallows', 'highwater_mark', 'locomotive', 'manor', 'memorial', 'milestone', 'monastery', 'monument', 
                                    'optical_telegraph', 'pa', 'pillory', 'railway_car', 'ruins', 'rune_stone', 'ship', 'tank', 'tomb', 'tower', 
                                    'vehicle', 'wayside_cross', 'wayside_shrine', 'wreck', 'yes'],
    
    'historic_pois'             :  [],

    'historic_landuse'          :  [],

    'historic_buildings'        :  [],

    'historic_street_network'   :  [],

    #
    'landuse'                   :  ['commercial', 'construction', 'education', 'industrial', 'residential', 'retail', 'allotments', 
                                    'farmland', 'farmyard', 'flowerbed', 'forest', 'meadow', 'orchard', 'vineyard', 'aquaculture', 'basin', 
                                    'reservoir', 'salt_pond', 'brownfield', 'cemetery', 'depot', 'garages', 'grass', 'greenfield', 
                                    'greenhouse_horticulture', 'landfill', 'military', 'plant_nursery', 'port', 'quarry', 'railway', 
                                    'recreation_ground', 'religious', 'village_green', 'winter_sports', ('user defined'), 
                                    'water', 'fallow', 'pasture', 'plantation', 'green_area', 'churchyard', 'highway', 'garden',
                                    'national_park', 'nature_reserve', 'park', 'grave_yard'],
    
    'landuse_pois'              :  ['forest'],

    'landuse_landuse'           :  ['commercial', 'construction', 'education', 'industrial', 'residential', 'retail', 'allotments', 
                                    'farmland', 'farmyard', 'flowerbed', 'forest', 'meadow', 'orchard', 'vineyard', 'aquaculture', 'basin', 
                                    'reservoir', 'salt_pond', 'brownfield', 'cemetery', 'depot', 'garages', 'grass', 'greenfield', 
                                    'greenhouse_horticulture', 'landfill', 'military', 'plant_nursery', 'port', 'quarry',  
                                    'recreation_ground', 'religious', 'village_green', 'winter_sports',
                                    'water', 'fallow', 'pasture', 'plantation', 'green_area', 'churchyard', 'highway'],

    'landuse_buildings'         :  [],

    'landuse_street_network'    :  ['railway'],

    #
    'leisure'                   :  ['adult_gaming_centre', 'amusement_arcade', 'beach_resort', 'bandstand', ('bird_hide'), 
                                    ('common'), 'dance', ('disc_golf_course'), 'dog_park', 'escape_game', ('firepit'), ('fishing'), 'fitness_centre', 
                                    ('fitness_station'), 'garden', ('hackerspace'), 'horse_riding', 'ice_rink', 'marina', 'miniature_golf', 
                                    'nature_reserve', 'park', ('picnic_table'), 'pitch', 'playground', ('slipway'), 'sports_centre', 'stadium', 
                                    ('summer_camp'), ('swimming_area'), 'swimming_pool', 'track', 'water_park', 'leisure'],
    
    'leisure_pois'              :  ['fitness_centre', 'park', 'pitch', 'playground', 'sports_centre', 'swimming_pool', 'water_park'],

    'leisure_landuse'           :  ['adult_gaming_centre', 'amusement_arcade', 'beach_resort', 'bandstand',
                                    'dance', 'dog_park', 'escape_game', 'fitness_centre',
                                    'garden', 'horse_riding', 'marina', 'miniature_golf', 
                                    'nature_reserve', 'park', 'pitch', 'playground', 'sports_centre', 'stadium',
                                    'swimming_pool', 'track', 'water_park', 'leisure'],

    'leisure_buildings'         :  ['adult_gaming_centre', 'amusement_arcade', 'bandstand',
                                    'dance', 'escape_game', 'fitness_centre',
                                    'sports_centre', 'stadium'],

    'leisure_street_network'    :  [],
                            
    #
    'man_made'                  :  [('adit'),( 'beacon'), ('breakwater'), 'bridge', ('bunker_silo'), ('carpet_hanger'), 'chimney', 
                                    'communications_tower', ('crane'), ('cross'), ('cutline'), ('clearcut'), ('dovecote'), ('dyke'), ('embankment'), ('flagpole'), 
                                    'gasometer', ('goods_conveyor'), ('groyne'), ('guard_stone'), ('kiln'), 'lighthouse', ('mast'), ('mineshaft'), 
                                    ('monitoring_station'), ('obelisk'), ('observatory'), ('offshore_platform'), ('petroleum_well'), 'pier', ('pipeline'), 
                                    ('pump'), ('pumping_station'), ('reservoir_covered'), 'silo', ('snow_fence'), ('snow_net'), 'storage_tank', 
                                    ('street_cabinet'), ('stupa'), ('surveillance'), ('survey_point'), ('tailings_pond'), ('telescope'), ('tower'), 
                                    ('video_wall'), 'wastewater_plant', ('watermill'), 'water_tower', ('water_well'), ('water_tap'), 'water_works', 
                                    ('wildlife_crossing'), 'windmill', 'works', ('yes')],
    
    'man_made_pois'             :  [],

    'man_made_landuse'          :  ['bridge', 'chimney',
                                    'communications_tower',
                                    'gasometer', 'lighthouse',
                                    'pier',
                                    'silo', 'storage_tank',
                                    'wastewater_plant', 'water_tower', 'water_works',
                                    'windmill', 'works'
                                    ],

    'man_made_buildings'        :  ['communications_tower',
                                    'gasometer', 'lighthouse',
                                    'silo', 'storage_tank'
                                    ],

    'man_made_street_network'   :  ['bridge'],

    #
    'military'                  :  ['airfield', 'base', 'bunker', 'barracks', ('checkpoint'), 'danger_area', 'naval_base', 
                                    'nuclear_explosion_site', ('obstacle_course'), 'office', 'range', 'training_area', ('trench')],

    'military_pois'             :  [],

    'military_landuse'          :  ['airfield', 'base', 'bunker', 'barracks', 'danger_area', 'naval_base',
                                    'nuclear_explosion_site', 'office', 'range', 'training_area'],

    'military_buildings'        :  ['barracks', 'office'],

    'military_street_network'   :  [],

    #
    'natural'                   :  ['wood', 'tree_row', ('tree'), 'scrub', 'heath', 'moor', 'grassland', 'fell', ('bare_rock'), 
                                    ('scree'), ('shingle'), ('sand'), ('mud'), 'water', 'wetland', ('glacier'), 'bay', 'strait', ('cape'), ('beach'), 
                                    ('coastline'), ('reef'), 'spring', 'hot_spring', 'geyser', 'blowhole', ('peak'), ('volcano'), ('valley'), ('peninsula'), 
                                    ('isthmus'), ('ridge'), ('arete'), ('cliff'), ('saddle'), ('dune'), ('rock'), ('stone'), ('sinkhole'), ('cave_entrance')],
    
    'natural_pois'              :  [],

    'natural_landuse'           :  ['wood', 'tree_row', 'scrub', 'heath', 'moor', 'grassland', 'fell', 'water', 'wetland', 'bay', 'strait',
                                    'spring', 'hot_spring', 'geyser', 'blowhole'],

    'natural_buildings'         :  [],

    'natural_street_network'    :  [],

    #
    'office'                    :  ['accountant', 'advertising_agency', 'architect', 'association', 'charity', 'company', 
                                    'consulting', 'courier', 'coworking', 'diplomatic', 'educational_institution', 'employment_agency', 
                                    'energy_supplier', 'engineer', 'estate_agent', 'financial', 'financial_advisor', 'forestry', 'foundation', 
                                    'geodesist', 'government', 'graphic_design', 'guide', 'harbour_master', 'insurance', 'it', 'lawyer', 
                                    'logistics', 'moving_company', 'newspaper', 'ngo', 'notary', 'political_party', 'property_management', 
                                    'quango', 'religion', 'research', 'security', 'surveyor', 'tax_advisor', 'telecommunication', 'travel_agent', 
                                    'union', 'visa', 'water_utility', ('yes')],
        
    'office_pois'              :  [],

    'office_landuse'           :   [ 'Attention single offices!'
                                    'accountant', 'advertising_agency', 'architect', 'association', 'charity', 'company', 
                                    'consulting', 'courier', 'coworking', 'diplomatic', 'educational_institution', 'employment_agency', 
                                    'energy_supplier', 'engineer', 'estate_agent', 'financial', 'financial_advisor', 'forestry', 'foundation', 
                                    'geodesist', 'government', 'graphic_design', 'guide', 'harbour_master', 'insurance', 'it', 'lawyer', 
                                    'logistics', 'moving_company', 'newspaper', 'ngo', 'notary', 'political_party', 'property_management', 
                                    'quango', 'religion', 'research', 'security', 'surveyor', 'tax_advisor', 'telecommunication', 'travel_agent', 
                                    'union', 'visa', 'water_utility'],

    'office_buildings'         :  [],

    'office_street_network'    :  [],

    # 
    'place'                     :  ['country', 'state', 'region', 'province', 'district', 'county', 'municipality', 'city', 'borough', 
                                    'suburb', 'quarter', 'neighbourhood', 'city_block', 'plot', 'town', 'village', 'hamlet', 'isolated_dwelling', 
                                    'farm', 'allotments', 'continent', 'archipelago', 'island', 'islet', 'square', 'locality', 'sea', 'ocean'],
    
    'place_pois'               :  [],

    'place_landuse'            :  ['farm'],

    'place_buildings'          :  [],

    'place_street_network'     :  [],    

    #
    'power'                     :  [('cable'), ('catenary_mast'), ('compensator'), ('converter'), ('generator'), ('heliostat'), ('insulator'), 
                                    ('line'), ('busbar'), ('bay'), ('minor_line'), 'plant', ('pole'), ('portal'), 'substation', ('switch'), 'switchgear', 
                                    ('terminal'), ('tower'), ('transformer')],
    
    'power_pois'                :  [],

    'power_landuse'             :  ['plant', 'substation', 'switchgear'],

    'power_buildings'           :  [],

    'power_street_network'      :  [],

    #
    'public_transport'          :  ['stop_position', 'platform', 'station', 'stop_area'],
        
    'public_transport_pois'             :  ['stop_position', 'platform', 'station', 'stop_area'],

    'public_transport_landuse'          :  ['stop_position', 'platform', 'station', 'stop_area'],

    'public_transport_buildings'        :  [],

    'public_transport_street_network'   :  ['stop_position', 'platform', 'station', 'stop_area'],

    #
    'railway'                   :  [('abandoned'), 'construction', ('disused'), 'funicular', 'light_rail', ('miniature'), 'monorail', 
                                    'narrow_gauge', ('preserved'), 'rail', 'subway', 'tram', 'halt', 'platform', 'station', 'subway_entrance', 
                                    'tram_stop', ('buffer_stop'), ('derail'), 'crossing', 'level_crossing', ('signal'), 
                                    ('switch'), ('railway_crossing'), ('turntable'), ('roundhouse'), ('traverser'), ('wash'), ('user defined')],
    
    'railway_pois'              :  ['halt', 'platform', 'station', 'subway_entrance',
                                    'tram_stop'],

    'railway_landuse'           :  ['construction', 'funicular', 'light_rail', 'monorail', 
                                    'narrow_gauge', 'rail', 'subway', 'tram', 'halt', 'platform', 'station', 'subway_entrance',
                                    'tram_stop', 'crossing', 'level_crossing'],

    'railway_buildings'         :  [],

    'railway_street_network'    :  ['construction', 'funicular', 'light_rail', 'monorail', 
                                    'narrow_gauge', 'rail', 'subway', 'tram', 'halt', 'platform', 'station', 'subway_entrance',
                                    'tram_stop', 'crossing', 'level_crossing'],

    # eventuell zu verwirrend in der finalen Karte
    'route'                     :  ['bicycle', 'bus', ('canoe'), 'detour', ('ferry'), 'foot', 'hiking', 'horse', ('inline_skates'), 
                                    'light_rail', 'mtb', 'piste', 'railway', 'road', 'running', 'ski', 'subway', 'train', 'tracks', 'tram', 
                                    ('trolleybus')],

    'route_pois'               :  [],

    'route_landuse'            :  ['bicycle', 'bus', 'detour', 'foot', 'hiking', 'horse', 
                                   'light_rail', 'mtb', 'piste', 'railway', 'road', 'running', 'ski', 'subway', 'train', 'tracks', 'tram'],

    'route_buildings'          :  [],

    'route_street_network'     :  ['bicycle', 'bus', 'detour', 'foot', 'hiking', 'horse', 
                                   'light_rail', 'mtb', 'piste', 'railway', 'road', 'running', 'ski', 'subway', 'train', 'tracks', 'tram'], 

    # fashion, drugstore, lamps duplicate of clothes; windows not used anymore
    'shop'                      :  ['alcohol', 'bakery', 'beverages', 'brewing_supplies', 'butcher', 'cheese', 'chocolate', 'coffee', 
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
                                    'tobacco', 'toys', 'travel_agency', ('vacant'), 'weapons', 'outpost'],
    
    'shop_pois'                 :  ['depends'],

    'shop_landuse'              :  ['alcohol', 'bakery', 'beverages', 'brewing_supplies', 'butcher', 'cheese', 'chocolate', 'coffee', 
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
                                    'tobacco', 'toys', 'travel_agency', 'weapons', 'outpost'],

    'shop_buildings'            :  ['alcohol', 'bakery', 'beverages', 'brewing_supplies', 'butcher', 'cheese', 'chocolate', 'coffee', 
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
                                    'tobacco', 'toys', 'travel_agency', 'weapons', 'outpost'],

    'shop_street_network'       :  [],    

    # maybe not use them at all as they are meant to refine leisure tags | non-physical tag
    'sport'                     :  ['9pin', '10pin', 'american_football', 'aikido', 'archery', 'athletics', 'australian_football', 
                                    'badminton', 'bandy', 'baseball', 'basketball', 'beachvolleyball', 'biathlon', 'billiards', 'bmx', 
                                    'bobsleigh', 'boules', 'bowls', 'boxing', 'bullfighting', 'canadian_football', 'canoe', 'chess', ('cliff_diving'), 
                                    'climbing', 'climbing_adventure', ('cockfighting'), 'cricket', 'crossfit', 'croquet', 'curling', ('cycle_polo'), 
                                    ('cycling'), ('darts'), ('dog_agility'), ('dog_racing'), 'equestrian', 'fencing', 'field_hockey', 'fitness', 
                                    'five-a-side', 'floorball', 'free_flying', 'futsal', 'gaelic_games', 'golf', 'gymnastics', 'handball', 
                                    'hapkido', ('horseshoes'), 'horse_racing', 'ice_hockey', 'ice_skating', 'ice_stock', 'jiu-jitsu', 'judo', 
                                    'karate', 'karting', 'kickboxing', ('kitesurfing'), 'korfball', 'krachtbal', 'lacrosse', 'martial_arts', 
                                    'miniature_golf', ('model_aerodrome'), 'motocross', 'motor', 'multi', 'netball', 'obstacle_course', 
                                    ('orienteering'), 'paddle_tennis', 'padel', ('parachuting'), ('parkour'), ('pedal_car_racing'), ('pelota'), 'pesäpallo', 
                                    'pickleball', 'pilates', 'pole_dance', 'racquet', ('rc_car'), ('roller_skating'), ('rowing'), 'rugby_league', 
                                    'rugby_union', ('running'), ('sailing'), ('scuba_diving'), 'shooting', 'shot-put', 'skateboard', 'ski_jumping', 
                                    'skiing', 'snooker', 'soccer', 'speedway', 'squash', 'sumo', ('surfing'), ('swimming'), 'table_tennis', 
                                    'table_soccer', 'taekwondo', 'tennis', 'toboggan', 'ultimate', 'volleyball', ('wakeboarding'), 'water_polo', 
                                    ('water_ski'), 'weightlifting', 'wrestling', 'yoga', 'zurkhaneh_sport'],
    
    'sport_pois'                :  [],

    'sport_landuse'             :  [],

    'sport_buildings'           :  [],

    'sport_street_network'      :  [],

    #
    'telecom'                   :  [('exchange'), ('connection_point'), ('distribution_point'), ('service_device'), 'data_center'],
        
    'telecom_pois'              :  [],

    'telecom_landuse'           :  ['data_center'],

    'telecom_buildings'         :  ['data_center'],

    'telecom_street_network'    :  [],

    #
    'tourism'                   :  ['alpine_hut', 'apartment', 'aquarium', ('artwork'), ('attraction'), ('camp_pitch'), 'camp_site', 
                                    'caravan_site', 'chalet', 'gallery', 'guest_house', 'hostel', 'hotel', ('information'), 'motel', 'museum', 
                                    ('picnic_site'), 'theme_park', ('viewpoint'), ('wilderness_hut'), 'zoo', ('yes')],
    
    'tourism_pois'              :  ['alpine_hut', 'apartment', ('aquarium'), ('artwork'), ('attraction'), 'camp_site', 
                                    'caravan_site', 'chalet', 'gallery', 'guest_house', 'hostel', 'hotel', 'motel', 'museum',
                                    'theme_park', 'zoo'],

    'tourism_landuse'           :  ['alpine_hut', 'apartment', 'aquarium', 'camp_site',
                                    'caravan_site', 'chalet', 'gallery', 'guest_house', 'hostel', 'hotel', 'motel', 'museum',
                                    'theme_park', 'zoo'],

    'tourism_buildings'         :  ['alpine_hut', 'aquarium',
                                    'chalet', 'gallery', 'guest_house', 'hostel', 'hotel', 'motel', 'museum'],

    'tourism_street_network'    :  [],

    #
    'water'                     :  ['river', 'oxbow', 'canal', 'ditch', ('lock'), ('fish_pass'), 'lake', 'reservoir', 'pond', 'basin', 
                                    'lagoon', 'stream_pool', 'reflecting_pool', 'moat', 'wastewater'],
    
    'water_pois'                :  [],

    'water_landuse'             :  ['river', 'oxbow', 'canal', 'ditch', 'lake', 'reservoir', 'pond', 'basin',
                                    'lagoon', 'stream_pool', 'reflecting_pool', 'moat', 'wastewater'],

    'water_buildings'           :  [],

    'water_street_network'      :  [],

    #
    'waterway'                  :  ['river', 'riverbank', 'stream', 'tidal_channel', 'canal', 'drain', 'ditch', ('pressurised'), 
                                    'fairway', 'dock', 'boatyard', ('dam'), ('weir'), 'waterfall', ('lock_gate'), ('soakhole'), ('turning_point'), 
                                    ('water_point'), ('fuel')],
    
    'waterway_pois'             :  [],

    'waterway_landuse'          :  ['river', 'riverbank', 'stream', 'tidal_channel', 'canal', 'drain', 'ditch',
                                    'fairway', 'dock', 'boatyard', 'waterfall'],

    'waterway_buildings'        :  [],

    'waterway_street_network'   :  [],

}