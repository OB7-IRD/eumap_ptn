SELECT
	c.topiaid::TEXT AS catch_id
	,EXTRACT (YEAR FROM s.settingendtimestamp)::integer AS setting_end_year
	,o.code::integer AS ocean_code
	,s2.faocode::text AS specie_code_fao
	,s2.scientificlabel::TEXT AS specie_name
	,c.count::integer AS count
	,s3."size"::NUMERIC AS SIZE
	,s4.code::TEXT AS size_type
	,w.weight::NUMERIC AS weight
	,s5.code::TEXT AS weight_type
	,s6.label1::TEXT AS sex
	,m.label1::TEXT AS maturity
FROM
	observe_longline.catch c
	INNER JOIN observe_longline."set" s ON (c."set" = s.topiaid)
	INNER JOIN observe_longline.activity a ON (a."set" = s.topiaid)
	INNER JOIN observe_longline.trip t ON (a.trip = t.topiaid)
	INNER JOIN observe_common.vessel v ON (t.vessel = v.topiaid)
	INNER JOIN observe_common.ocean o ON (t.ocean = o.topiaid)
	INNER JOIN observe_common.species s2 ON (s2.topiaid = c.speciescatch)
	LEFT JOIN observe_longline.sizemeasure s3 ON (c.topiaid = s3.catch)
	LEFT JOIN observe_common.sizemeasuretype s4 ON (s3.sizemeasuretype = s4.topiaid)
	LEFT JOIN observe_longline.weightmeasure w ON (c.topiaid = w.catch)
	LEFT JOIN observe_common.sizemeasuretype s5 ON (w.weightmeasuretype = s4.topiaid)
	LEFT JOIN observe_common.sex s6 ON (c.sex = s6.topiaid)
	LEFT JOIN observe_longline.maturitystatus m ON (c.maturitystatus = m.topiaid)
WHERE
	EXTRACT (YEAR FROM s.haulingendtimestamp) IN (?period)
	AND v.fleetcountry IN (?countries)
;
