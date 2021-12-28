import sys
sys.path.insert(0,"..")
import collection

config_population = collection.Config("population")
variable_container_population = config_population.variable_container

meter_degree = f'''

DROP FUNCTION IF EXISTS meter_degree();
CREATE OR REPLACE FUNCTION public.meter_degree()
RETURNS NUMERIC AS
$$
	SELECT {variable_container_population['one_meter_degree']}::numeric;
$$
LANGUAGE sql;

--SELECT meter_degree() 

'''