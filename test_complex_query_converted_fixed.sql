select
	/* Core values to pull for the agent results */
	  c."npn",
	  c."agent-name",
	/* Broker */
	  c.broker,
	  c."broker-master-id",
	  c.parent,
	  c."parent-master-id",
	  c.association,
	  c."association-master-id",
	  c."broker-cb-domain",
	  c."broker-general-agent-flag",
	  c."is-minor-broker-flag",
	  c."broker-master-office-id",
	  c."office-name",
	  c."office-address-line-1",
	  c."office-address-line-2",
	  c."office-address-city",
	  c."office-address-state",
	  c."office-address-zip",
	  c."office-address-county",
	  c."office-latitude",
	  c."office-longitude",
	  c."prepared-email-business-type",
	  c."modeled-email-flag",
	  c."primary_phone",
	  c."eligible-for-qualification",
	  c."linkedin-urn",
	  c."linkedin-url",
	  c."linkedin-refresh-date",
	  c."linkedin-location-city",
	  c."linkedin-location-state",
	  c."linkedin-location-top-zip",
	  c."linkedin-location-longitude",
	  c."linkedin-location-latitude",
	  c."linkedin-about",
	  c."linkedin-website",
	  c."linkedin-headline",
	  c."linkedin-followers",
	  c."linkedin-full_name",
	  c."linkedin-connections",
	  c."linkedin-experience-affiliation",
	  c."linkedin-experience-company",
	  c."linkedin-current-title",
	  c."linkedin-experience-location",
	  c."linkedin-experience-start",
	  c."linkedin-experience-end",
	  c."linkedin-started-with-company-months-ago",
	  c."linkedin-title-started-months-ago",

	/* Custom fields
	   Must be aggregated to the grouping level, use array_agg generally */
	array_agg(distinct CASE WHEN c."lincoln-broker-office" is not null THEN c."lincoln-broker-office" ELSE NULL END) as "lincoln-broker-office",

	array_agg(distinct CASE WHEN c."lincoln-broker-region" is not null THEN c."lincoln-broker-region" ELSE NULL END) as "lincoln-broker-region",

	/* Inject all aggregate clauses here */

	/* Take the minimum general agent flag, which should be showing 'General Agent' before 'Not General Agent' */
	min(c."general-agent-flag")                                                           as "general-agent-flag",
	/* Agent aggregate stats */
	count(distinct c."company-id")::DOUBLE precision                                      as count_employers,
	count(*)::DOUBLE precision                                                            as count_plans,
	sum(case
			when c."modeled-broker-flag" = 'Yes'
				then 1
		end)::DOUBLE precision                                                            as count_modeled_plans,
	avg(case
		when c."competitive-status" = 'expansion'
			then 1
		when c."competitive-status" = 'contraction'
			then -1
	end)::DOUBLE precision                                                                as expansion_vs_contraction_plan_count_skew,
	round(avg(c."years-relationship")::DOUBLE precision)                                  as average_years_relationship_with_employers,
	mode() within group (order by c.role)                                                 as most_common_agent_role,
	sum(case
			when c."general-agent-flag" = 'General Agent'
				then 1
		end)::DOUBLE precision /
	count(*)::DOUBLE precision                                                            as ratio_of_general_agent_activity,
	avg(c."attributed-premium")::DOUBLE precision                                         as avg_plan_attributed_premium,
	sum(c."attributed-premium")::DOUBLE precision                                         as sum_plan_attributed_premium,
	sum(c.commissions)::DOUBLE precision                                                  as sum_plan_commissions,
	avg(c."commission-rate")::DOUBLE precision                                            as avg_plan_commission_rate,
	sum(c.fees)::DOUBLE precision                                                         as sum_plan_fees,
	avg(c."fee-rate")::DOUBLE precision                                                   as avg_plan_fee_rate,
	sum(c."total-compensation")::DOUBLE precision                                         as sum_plan_total_compensation,
	avg(c."total-compensation-rate")::DOUBLE precision                                    as avg_plan_total_compensation_rate,
	avg(c."total-compensation-share")::DOUBLE precision                                   as avg_plan_total_compensation_share,
	mode() within group (order by c."compensation-rate-category")                as most_common_compensation_rate_category,
	/* Plan aggregate stats */
	array_agg(distinct CASE WHEN c."full-state" is not null THEN c."full-state" ELSE NULL END)                                    as all_plan_states,
	public.array_union(CASE WHEN c.products is not null THEN c.products ELSE 0 END)                                        as all_plan_products,
	array_agg(distinct CASE WHEN c."carrier-name" is not null THEN c."carrier-name" ELSE NULL END)                                  as all_carriers,
	sum(c."employee-count")::DOUBLE precision                                    as sum_plan_employees,
	sum(c.participants)::DOUBLE precision                                        as sum_plan_participants,
	sum(c.premium)::DOUBLE precision                                             as sum_plan_premium,

	/* Precalculated platform values */
	  concat('http://localhost:3000', replace('/agents/[npn]', '[npn]', c."npn"::text)) as "benefeature-link",
	  (count(*) over ())::integer as total_count
from
  (
	select cc.*
	from (select
			  /* Agent attribute columns */
			  ag.npn,
			  ag."agent-name",
			  ag.discoverycontactid,
			  ag.discoverydataprofileurl,
			  ag."broker-master-name",
			  ag."broker-name-cleansed",
			  ag."broker-affiliation",
			  ag."broker-cb-domain",
			  ag."broker-parent-master-id",
			  ag."broker-association-master-id",
			  ag."broker-is-parent",
			  ag."broker-is-association",
			  ag."broker-general-agent-flag",
			  ag."broker-master-office-id",
			  ag."office-address-line-1",
			  ag."office-address-line-2",
			  ag."office-address-city",
			  ag."office-address-state",
			  ag."office-address-zip",
			  ag."office-address-county",
			  ag."office-latitude",
			  ag."office-longitude",
			  ag."office-geometry-point",
			  ag."prepared-email-business-type",
			  ag."modeled-email-flag",
			  ag.primary_phone,
			  ag."eligible-for-qualification",
			  ag."linkedin-urn",
			  ag."linkedin-url",
			  ag."linkedin-refresh-date",
			  ag."linkedin-location-city",
			  ag."linkedin-location-state",
			  ag."linkedin-location-top-zip",
			  ag."linkedin-location-longitude",
			  ag."linkedin-location-latitude",
			  ag."linkedin-about",
			  ag."linkedin-website",
			  ag."linkedin-headline",
			  ag."linkedin-followers",
			  ag."linkedin-full_name",
			  ag."linkedin-connections",
			  ag."linkedin-experience-affiliation",
			  ag."linkedin-experience-company",
			  ag."linkedin-current-title",
			  ag."linkedin-experience-location",
			  ag."linkedin-experience-start",
			  ag."linkedin-experience-end",
			  ag."linkedin-started-with-company-ago-band",
			  ag."linkedin-started-with-company-months-ago",
			  ag."linkedin-title-started-ago-band",
			  ag."linkedin-title-started-months-ago",
			  ag."prior-role-title",
			  ag."prior-role-company",
			  ag."prior-role-logo_url",
			  ag."prior-role-starts_at",
			  ag."prior-role-ends_at",
			  ag."prior-role-broker-master-id",
			  ag."prior-role-broker-master-name",
			  ag."prior-role-carrier-master-id",
			  ag."prior-role-carrier-master-name",
			  ag."prior-role-months_with_company",
			  ag."prior-role-months_in_role",
			  /* Windowed aggregates for some market-level filters */
			  /* All carriers
				 Do not bother with selecting distinct values here, outer will re-aggregate carriers appropriately */
			  array_agg(cc_inner."carrier-name")
			  over (partition by ag."npn") as all_carriers,
			  /* All core columns */
			  cc_inner.*
		  from insurance_plans_core_brokers            as cc_inner
			  cross join lateral (select y as agent
								  from jsonb_array_elements(cc_inner."qualified_agents") as y
								  where y is not null) as cj_ag
			  join agents                              as ag
				on ag.npn = (JSON_VALUE(JSON_POINTER_EXTRACT_TEXT(cj_ag.agent, '/npn')))::integer
   ) as cc
   where 1 = 1 /* Placeholder for potential empty dynamic filtering */
   /* Apply broker IDs if defined */

   /* Dynamic conditions */

   /* Static filtering */

	  /* Filter out any agents which are considered to be "minor" */
	  and "is-minor-broker-flag" = 0
	  /* Checks whether the name is populated */
	  and length("agent-name") > 0
   /* Territory filtering */

  ) as "c"
/* For grouping, don't do rollups, they're very slow
   Just run the query on each basis if other perspectives are required */
group by
	/* Core values to pull for the agent results */
	  c."npn",
	  c."agent-name",
	/* Broker */
	  c.broker,
	  c."broker-master-id",
	  c.parent,
	  c."parent-master-id",
	  c.association,
	  c."association-master-id",
	  c."broker-cb-domain",
	  c."broker-general-agent-flag",
	  c."is-minor-broker-flag",
	  c."broker-master-office-id",
	  c."office-name",
	  c."office-address-line-1",
	  c."office-address-line-2",
	  c."office-address-city",
	  c."office-address-state",
	  c."office-address-zip",
	  c."office-address-county",
	  c."office-latitude",
	  c."office-longitude",
	  c."prepared-email-business-type",
	  c."modeled-email-flag",
	  c."primary_phone",
	  c."eligible-for-qualification",
	  c."linkedin-urn",
	  c."linkedin-url",
	  c."linkedin-refresh-date",
	  c."linkedin-location-city",
	  c."linkedin-location-state",
	  c."linkedin-location-top-zip",
	  c."linkedin-location-longitude",
	  c."linkedin-location-latitude",
	  c."linkedin-about",
	  c."linkedin-website",
	  c."linkedin-headline",
	  c."linkedin-followers",
	  c."linkedin-full_name",
	  c."linkedin-connections",
	  c."linkedin-experience-affiliation",
	  c."linkedin-experience-company",
	  c."linkedin-current-title",
	  c."linkedin-experience-location",
	  c."linkedin-experience-start",
	  c."linkedin-experience-end",
	  c."linkedin-started-with-company-months-ago",
	  c."linkedin-title-started-months-ago"
order by
/* Order by clause */
	  /* Default sort first on whether the agent was enriched successfully */
		case when c."linkedin-current-title" is not null then 0 else 1 end,

	  /* Default on total plans next */
		"count_plans" desc nulls last,
		c."linkedin-full_name",
		c."agent-name",
		c."npn"
/* Offset and limit clauses */
offset 0 limit 25;