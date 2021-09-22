SELECT
	DISTINCT EXTRACT (YEAR FROM t.landingdate)::integer AS landing_year	
	,v.label1::TEXT AS vessel_name
	--,c.label1::TEXT AS compagny
FROM
	public.trip t
	INNER JOIN public.vessel v ON (t.vessel = v.topiaid)
	INNER JOIN public.vesseltype v2 ON (v.vesseltype = v2.topiaid)
	INNER JOIN public.vesselsimpletype v3 ON (v2.vesselsimpletype = v3.topiaid)
	--LEFT JOIN public.company c ON (v.company = c.topiaid)
	INNER JOIN public.country c2 ON (v.flagcountry = c2.topiaid)
WHERE
	v3.code = 1
	AND EXTRACT (YEAR FROM t.landingdate) IN (2018, 2019, 2020, 2021)
	AND c2.code IN (1, 41)
;