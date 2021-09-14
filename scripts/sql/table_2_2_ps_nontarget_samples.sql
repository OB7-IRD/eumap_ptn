SELECT
	EXTRACT (YEAR FROM r."date")::integer AS sampling_year
	,o.code::integer AS ocean_code
	,s2.faocode::TEXT AS specie_code
	,s2.scientificlabel::TEXT AS specie_name
	,t2.length::integer AS length
	,t2.weight::NUMERIC AS weight
	,t2.count::integer AS count
	,s3.label1::TEXT AS sex
FROM
	 observe_seine.nontargetsample n 
	 INNER JOIN observe_seine."set" s ON (n."set" = s.topiaid)
	 INNER JOIN observe_seine.activity a ON (s.topiaid = a."set")
	 INNER JOIN observe_seine.route r ON (a.route = r.topiaid)
	 INNER JOIN observe_seine.trip t ON (r.trip = t.topiaid)
	 INNER JOIN observe_common.ocean o ON (t.ocean = o.topiaid)
	 INNER JOIN observe_common.vessel v ON (t.vessel = v.topiaid)
	 INNER JOIN observe_seine.nontargetlength t2 ON (n.topiaid = t2.nontargetsample)
	 INNER JOIN observe_common.species s2 ON (t2.species = s2.topiaid)
	 LEFT JOIN observe_common.sex s3 ON (t2.sex = s3.topiaid)
WHERE
	EXTRACT (YEAR FROM r."date") IN (?period)
	AND v.fleetcountry IN (?countries)
;
