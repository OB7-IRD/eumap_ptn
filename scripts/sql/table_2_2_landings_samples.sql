SELECT
	EXTRACT(YEAR FROM t.landingdate)::integer AS landing_year
	,o.code::integer AS ocean_code
	,s3.code3l::TEXT AS specie_code
	,s3.scientificlabel::TEXT AS specie_name
	,s4.lengthclass::integer AS length_class
	,s4.number::integer AS count
FROM
	public.sample s
	INNER JOIN public.trip t ON (s.trip = t.topiaid)
	INNER JOIN public.harbour h ON (t.landingharbour = h.topiaid)
	INNER JOIN public.ocean o ON (h.ocean = o.topiaid)
	INNER JOIN public.vessel v ON (t.vessel = v.topiaid)
	INNER JOIN public.country c ON (v.flagcountry = c.topiaid)
	INNER JOIN public.samplespecies s2 ON (s2.sample = s.topiaid)
	INNER JOIN public.species s3 ON (s2.species = s3.topiaid)
	INNER JOIN public.samplespeciesfrequency s4 ON (s2.topiaid = s4.samplespecies)
WHERE
	EXTRACT(YEAR FROM t.landingdate) IN (?period)
	AND c.code IN (?countries)
;
