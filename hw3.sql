--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
--We want to create a daily report to track:
---Total unique sessions
---The average length of sessions in seconds
---The average number of searches completed before displaying a recipe 
---The ID of the recipe that was most viewed 

WITH top_recipe_by_day
AS (
	SELECT EVENT_DATE, RECIPE_ID
	FROM (
		SELECT event_date, RECIPE_ID, RVD, ROW_NUMBER() OVER (PARTITION BY EVENT_DATE ORDER BY EVENT_DATE) rn
		FROM (
			SELECT DISTINCT DATE_TRUNC('day', EVENT_TIMESTAMP) event_date, trim(PARSE_JSON(event_details) : "recipe_id", '*') AS recipe_id, COUNT(*) OVER (PARTITION BY DATE_TRUNC('day', EVENT_TIMESTAMP), RECIPE_ID) AS RVD
			FROM vk_data.events.website_activity
			WHERE recipe_id IS NOT NULL
			ORDER BY event_date ASC
			) QUALIFY RVD = MAX(RVD) OVER (PARTITION BY event_date)
		ORDER BY EVENT_DATE ASC
		)
	WHERE RN = 1
	),

base_sessions
AS (
	SELECT SESSION_ID
		,DATE_TRUNC('day', EVENT_TIMESTAMP) event_day
		,datediff('second', min(event_timestamp) OVER (PARTITION BY session_id)
		,max(event_timestamp) OVER (PARTITION BY session_id)) AS session_dur
		,CASE WHEN trim(PARSE_JSON(event_details) : "event", '*') = 'search' 
			AND EVENT_TIMESTAMP < (min(CASE WHEN trim(PARSE_JSON(event_details) : "event", '*') = 'view_recipe' 
			THEN event_timestamp ELSE NULL END) OVER (PARTITION BY session_id)) 
				THEN 1 ELSE NULL END AS FRV, MAX(CASE WHEN trim(PARSE_JSON(event_details) : "event", '*') = 'view_recipe' 
					THEN 1 ELSE 0 END) OVER (PARTITION BY SESSION_ID) AS rv_session
		,Count(DISTINCT Session_ID) OVER (PARTITION BY EVENT_DAY) AS spd
	FROM vk_data.events.website_activity
	ORDER BY EVENT_TIMESTAMP
	), 

recipe_view_sessions
AS (
	SELECT event_day, sum(frv) / count(DISTINCT session_id) AS avg_searches
	FROM base_sessions
	WHERE rv_session = 1
	GROUP BY 1
	ORDER BY event_day
	),

avg_session_dur
AS (
	SELECT event_day, avg(session_dur) avg_session_dur
	FROM (
		SELECT DISTINCT session_id, event_day, session_dur
		FROM base_sessions
		WHERE session_dur != 0
		)
	GROUP BY 1
	)

    
SELECT bs.event_day
	,bs.spd AS unique_sessions
	,asd.avg_session_dur avg_len_sess
	,rvs.avg_searches avg_searches_b4_recipe
	,trbd.RECIPE_ID AS most_ppopular_recipe_on_day
FROM base_sessions bs
LEFT JOIN top_recipe_by_day trbd ON trbd.event_date = bs.event_day
LEFT JOIN recipe_view_sessions rvs ON rvs.event_day = bs.event_day
LEFT JOIN avg_session_dur asd ON asd.event_day = bs.event_day
GROUP BY ALL
ORDER BY event_day
