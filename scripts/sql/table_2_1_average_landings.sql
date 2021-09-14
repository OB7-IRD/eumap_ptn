SELECT 
	EXTRACT(YEAR FROM t.landingdate)::integer AS landing_year
	,o.code::integer AS ocean_code
	,h.label1 
	,s.code3l::TEXT AS specie_code
	,s.scientificlabel::TEXT AS specie_name
	,e.weight::NUMERIC AS weight_t
FROM	
	public.elementarylanding e
 	INNER JOIN public.trip t ON (e.trip = t.topiaid)
 	INNER JOIN public.weightcategorylanding w ON (e.weightcategorylanding = w.topiaid)
 	INNER JOIN public.species s ON (w.species = s.topiaid)
 	INNER JOIN public.vessel v ON (t.vessel = v.topiaid)
 	INNER JOIN public.country c ON (v.flagcountry = c.topiaid)
 	INNER JOIN public.harbour h ON (t.landingharbour = h.topiaid)
 	LEFT JOIN public.ocean o ON (h.ocean = o.topiaid)
 WHERE
 	EXTRACT(YEAR FROM t.landingdate) IN (?period)
 	AND c.code IN (?countries)
 ORDER BY
 	landing_year
 	,specie_code
 	,specie_name
;
