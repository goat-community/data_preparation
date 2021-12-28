from pathlib import Path
import yaml

with open(Path.cwd().parent/'config/config.yaml', encoding="utf-8") as stream:
    config = yaml.safe_load(stream)
var = config['Population']


meter_degree = f'''

DROP FUNCTION IF EXISTS meter_degree();
CREATE OR REPLACE FUNCTION public.meter_degree()
RETURNS NUMERIC AS
$$
	SELECT {var['variable_container']['one_meter_degree']}::numeric;
$$
LANGUAGE sql;

--SELECT meter_degree() 

'''