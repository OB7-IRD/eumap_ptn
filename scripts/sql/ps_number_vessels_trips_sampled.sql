SELECT
	EXTRACT (YEAR FROM t.enddate)::integer AS end_date
	,o.label1::TEXT AS ocean_name
	,count(*)::integer AS number_trips
FROM
	observe_seine.trip t
	INNER JOIN observe_common.vessel v ON (t.vessel = v.topiaid)
	INNER JOIN observe_common.vesseltype v2 ON (v.vesseltype = v2.topiaid)
	INNER JOIN observe_common.ocean o ON (t.ocean = o.topiaid)
	INNER JOIN observe_common."program" p ON (t."program" = p.topiaid)
WHERE
	EXTRACT (YEAR FROM t.enddate) IN (2017, 2018, 2019)
	AND v2.code IN ('4', '5', '6')
	AND v.status = 1
	AND v.fleetcountry IN (1, 41)
	AND p.code = '5'
GROUP BY
	end_date
	,ocean_name
ORDER BY
	ocean_name
	,end_date
;
