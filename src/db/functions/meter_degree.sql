DROP FUNCTION IF EXISTS public.meter_degree();
CREATE OR REPLACE FUNCTION public.meter_degree()
RETURNS NUMERIC AS
$$
	SELECT {variable_container_population['one_meter_degree']}::numeric;
$$
LANGUAGE sql;

--SELECT meter_degree() 
