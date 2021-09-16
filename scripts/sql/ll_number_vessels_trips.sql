WITH number_vessel AS (
SELECT
	vessel_id_year_length.end_date
	,vessel_id_year_length.vessel_length
	,count(vessel_id_year_length.vessel_code)::integer AS number_vessels
FROM (SELECT
	DISTINCT EXTRACT (YEAR FROM t.enddate)::integer AS end_date
	,v.code::integer AS vessel_code
	,CASE 
		WHEN v.length < 10 THEN 'VL0810'
		WHEN v.length >= 10 AND v.length < 12 THEN 'VL1012'
		WHEN v.length >= 12 AND v.length < 18 THEN 'VL1218'
		ELSE 'VL1824'
	END::TEXT AS vessel_length
FROM
	observe_longline.trip t
	INNER JOIN observe_common.vessel v ON (t.vessel = v.topiaid)
	INNER JOIN observe_common.vesseltype v2 ON (v.vesseltype = v2.topiaid)
WHERE
	EXTRACT (YEAR FROM t.enddate) IN (2017, 2018, 2019)
	AND v2.code IN ('7', '13')
	AND v.status = 1
	AND v.fleetcountry IN (1, 41)
ORDER BY
	end_date
	,vessel_length) AS vessel_id_year_length
GROUP BY
	end_date
	,vessel_length),
number_trip AS (
SELECT
	EXTRACT (YEAR FROM t.enddate)::integer AS end_date
	,CASE 
		WHEN v.length < 10 THEN 'VL0810'
		WHEN v.length >= 10 AND v.length < 12 THEN 'VL1012'
		WHEN v.length >= 12 AND v.length < 18 THEN 'VL1218'
		ELSE 'VL1824'
	END::TEXT AS vessel_length
	,count(t.topiaid) AS number_trips
FROM
	observe_longline.trip t
	INNER JOIN observe_common.vessel v ON (t.vessel = v.topiaid)
	INNER JOIN observe_common.vesseltype v2 ON (v.vesseltype = v2.topiaid)
WHERE
	EXTRACT (YEAR FROM t.enddate) IN (2017, 2018, 2019)
	AND v2.code IN ('7', '13')
	AND v.status = 1
	AND v.fleetcountry IN (1, 41)
GROUP BY
	end_date
	,vessel_length
ORDER BY
	end_date
	,vessel_length)
SELECT 
	nv.end_date
	,nv.vessel_length
	,nv.number_vessels
	,nt.number_trips
FROM
	number_vessel nv
	INNER JOIN number_trip nt ON (nv.end_date = nt.end_date AND nv.vessel_length = nt.vessel_length)
;
