SELECT
	EXTRACT(YEAR FROM t.landingdate)::integer AS landing_year
	,o.code::integer AS ocean_code
	,h.label1::text AS harbour_name
	,v3.code::integer AS vessel_type_code
	,v3.label1::TEXT AS vessel_type_name
FROM
	public.trip t
	INNER JOIN public.vessel v ON (t.vessel = v.topiaid)
 	INNER JOIN public.country c ON (v.flagcountry = c.topiaid)
 	INNER JOIN public.harbour h ON (t.landingharbour = h.topiaid)
 	LEFT JOIN public.ocean o ON (h.ocean = o.topiaid)
 	INNER JOIN public.vesseltype v2 ON (v.vesseltype = v2.topiaid)
 	INNER JOIN public.vesselsimpletype v3 ON (v2.vesselsimpletype = v3.topiaid)
WHERE
 	EXTRACT(YEAR FROM t.landingdate) IN (?period)
 	AND c.code IN (?countries)
;
