set serveroutput on;
set define off;
​
prompt ----- ST received_date update -----
​
declare
​
--constants
  c_state_id                  stbs.state.state_id%type;
  c_state_code                stbs.state.state_code%type :=  'ST';
​
--cursors
  cursor r_date is
  select
          license_id,
          npn,
          scn,
          license_class,
          st_raw_date,
          st_received_date,
          stbs_received_date
  from
  (select
          l.license_id as license_id,
          a.npn as npn,
          ia.scn as scn,
          lc.license_class_desc as license_class,
          case
            when lc.license_class_desc = 'Prod' then ag.agrecddt
            when lc.license_class_desc = 'S Lines' then el.elrecddt
            when lc.license_class_desc = 'Adjust' then pa.parecddt
            else null
          end as st_raw_date,
          case
            when lc.license_class_desc = 'Prod' then to_date(decode(ag.agrecddt,'00000000',null, ag.agrecddt), 'YYYYMMDD')
            when lc.license_class_desc = 'S Lines' then to_date(decode(el.elrecddt,'00000000',null, el.elrecddt), 'YYYYMMDD')
            when lc.license_class_desc = 'Adjust' then to_date(decode(pa.parecddt,'00000000',null, pa.parecddt), 'YYYYMMDD')
            else null
          end as st_received_date,
          l.received_date as stbs_received_date,
          ag.agrecddt as agent_received_date,
          ay.ayrecddt as agency_received_date,
          el.elrecddt as ex_surplus_received_date,
          pa.parecddt as public_adj_received_date,
          vc.vcrecddt as viatical_received_date
  from stbs.individual i
  join stbs.identity_alias  ia  on  i.global_entity_id  = ia.global_entity_id
  join stbs.agent a on i.individual_id = a.individual_id
  join stbs.license l on a.agent_id = l.agent_id
  join stbs.license_status ls on l.license_status_id = ls.license_status_id
  join stbs.license_class lc on l.license_class_id = lc.license_class_id
  join stbs.state s on l.jurisdiction_id = s.state_id
  left join stbs.organization    o   on  a.organization_id  = o.organization_id
  left join stdata.ag002 ag on  nvl(ia.scn, o.oin)  = ag.pridno
  left join stdata.ay002 ay on  nvl(ia.scn, o.oin)  = ay.pridno
  left join stdata.el002 el on  nvl(ia.scn, o.oin)  = el.pridno
  left join stdata.pa002 pa on  nvl(ia.scn, o.oin)  = pa.pridno
  left join stdata.vc002 vc on  nvl(ia.scn, o.oin)  = vc.pridno
  where l.jurisdiction_id = (
                              select state_id
                              from stbs.state
                              where state_code = 'ST'
                            )
  and ls.license_status_code  in  ('Y','N')
  and l.received_date is null
  and (
        (ag.agrecddt is not null and ag.agrecddt != '00000000')
         or (ay.ayrecddt is not null and ay.ayrecddt != '00000000')
         or (el.elrecddt is not null and el.elrecddt != '00000000')
         or (pa.parecddt is not null and pa.parecddt != '00000000')
         or (vc.vcrecddt is not null and vc.vcrecddt != '00000000')
       )
  and lc.license_class_desc in (
                                'Prod',
                                'S Liness',
                                'Adjust'
                               )
  and ag.pridno is null
  ) sub
where st_raw_date != '00000000';
​
begin
​
    for cur_r in r_date
    loop
​
      begin
​
        update stbs.license
        set received_date       = cur_r.st_received_date
        ,   user_last_modified  = 'TSTRAWN'
        ,   date_last_modified  = sysdate
        ,   process_type = 'DBA_REQUEST_49766'
        where license_id        = cur_r.license_id
        and   received_date     is null;
​
      end;
​
    end loop; --cur_r
​
end;
