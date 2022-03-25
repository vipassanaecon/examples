alter session set current_schema = stbs;
set serveroutput on;
set define off;
​
prompt ----- START License class-----
​
declare
​
--constants
  c_state_id                  state.state_id%type;
  c_state_code                state.state_code%type;
​
--variables
  v_record_counter            number(10)  :=  0;
  v_ind_count                 number(10)  :=  0;
  v_be_count                  number(10)  :=  0;
​
--environment variables
  is_real_migration           boolean  :=  true;
​
--row types
  r_migration_log             migration_log%rowtype;
  r_license_class             license_class%rowtype;
  r_license_class_expiration  license_class_expiration%rowtype;
  r_grace_period              grace_period%rowtype;
​
--cursors
  cursor license_classes is
        select distinct
              decode(trim(ddval_cd), 'BANR', 'BB', 'AJ', 'ADJ', 'BA', 'BB', 'AJDHS', 'ADJ',  'RPRDCR', 'PRO', 'NPRDCR', 'PRO', 'RPAJ', 'PAD', 'NPAJ', 'PAD', 'TRPRDCR', 'TMP',
                                    'RSL', 'SLB', 'NSL', 'SLB', 'LSEB', 'LSB', 'LSEP', 'LSP', 'MGA', 'MGA') as license_class,
              case
                   when trim(long_dscr) = 'Independent Adjuster - Designated' then 'Adjuster'
                   else regexp_replace (trim(regexp_replace (trim(long_dscr), '^(\S*)', '')), ' Individual','')
              end as license_class_desc,
              null expiration_month,
              null odd_even,
              decode(end_dte, null, 'Y','N') as isactive,
              min(beg_dte) as eff_date,
              max(end_dte) as term_date
        from iddata.codelist
        where cddmn_name = 'LIC_TYPE_CD'
        and not exists
          (
          select 1
          from license_class
          where jurisdiction_id = c_state_id
          and   license_class   =  decode(trim(ddval_cd), 'BANR', 'BB', 'AJ', 'ADJ', 'BA', 'BB', 'AJDHS', 'ADJ',  'RPRDCR', 'PRO', 'NPRDCR', 'PRO', 'RPAJ', 'PAD', 'NPAJ', 'PAD', 'TRPRDCR', 'TMP',
                                'RSL', 'SLB', 'NSL', 'SLB', 'LSEB', 'LSB', 'LSEP', 'LSP', 'MGA', 'MGA')
          )
          group by
          decode(trim(ddval_cd), 'BANR', 'BB', 'AJ', 'ADJ', 'BA', 'BB', 'AJDHS', 'ADJ',  'RPRDCR', 'PRO', 'NPRDCR', 'PRO', 'RPAJ', 'PAD', 'NPAJ', 'PAD', 'TRPRDCR', 'TMP',
                                'RSL', 'SLB', 'NSL', 'SLB', 'LSEB', 'LSB', 'LSEP', 'LSP', 'MGA', 'MGA'),
          case
             when trim(long_dscr) = 'Independent Adjuster - Designated' then 'Adjuster'
             else REGEXP_REPLACE (trim(regexp_replace (trim(long_dscr), '^(\S*)', '')), ' Individual','')
          end,
          decode(end_dte, null, 'Y','N');
​
--procedures
  procedure p_migration_log (p_activity in varchar2) is pragma autonomous_transaction;
    begin
      if is_real_migration then
        r_migration_log.migration_log_id := migration_log_id.nextval;
        r_migration_log.jurisdiction_id := c_state_id;
        r_migration_log.script_name := 'producer_config_license_class';
        r_migration_log.script_timestamp := systimestamp;
        r_migration_log.activity := p_activity;
        r_migration_log.process_type := null;
        r_migration_log.version_ol := 0;
        insert into migration_log values r_migration_log;
        commit;
      else
        dbms_output.put_line(p_activity||' >> '||to_char(systimestamp,'MM/DD/YY HH24:MI:SS.FF3'));
      end if;
    end;
​
begin
​
  select state_id, state_code into c_state_id, c_state_code
  from state
  where state_code  = 'ST';
​
  p_migration_log ('START');
​
  for cur_lc in license_classes
  loop
​
    /*select case when cur_lc.license_class in ('PRO','LLP','PED') then 'A'
                else 'I'
                end into r_license_class.entity_type_ind from dual;
​
    select case when cur_lc.license_class in ('AGT','BRK','VSB','LSB','REI','SLB') then 'N'
                else 'Y'
                end into r_license_class.ind_loa_reqd from dual;
​
    select case when cur_lc.license_class in ('LLP','PED') then 'Y'
                else 'N'
                end into r_license_class.be_loa_reqd from dual;
​
    -- appt allowed
    select decode(cur_lc.license_class, 'PRO','Y','LLP','Y','TTL','Y','BBA','Y','TMP','Y','MGA','Y','N')
    into r_license_class.appt_allowed from dual;*/
​
    r_license_class.license_class_id := license_class_id.nextval;
    r_license_class.jurisdiction_id := c_state_id;
    r_license_class.license_class := cur_lc.license_class;
    r_license_class.license_class_desc := cur_lc.license_class_desc;
    r_license_class.user_created := 'migration';
    r_license_class.date_created := sysdate;
    r_license_class.user_last_modified := 'migration';
    r_license_class.date_last_modified := sysdate;
    r_license_class.entity_type_ind := 'A';
    r_license_class.isdrlprequired := 'N';
    r_license_class.effective_date := cur_lc.eff_date;
    r_license_class.termination_date := cur_lc.term_date;
    r_license_class.appt_allowed := 'N';
    r_license_class.pdb_code := null;
    r_license_class.appt_level := 'LI';
    r_license_class.be_loa_rel_type_id := null;
    r_license_class.branch_office_ind := 'N';
    r_license_class.dhs_ind := 'N';
    r_license_class.iscerequired := 'N';
    r_license_class.fedexchflag := null;
    r_license_class.targets_by_loa := 'N';
    r_license_class.residency_ind := null;
    r_license_class.ind_loa_rel_type_id := null;
    r_license_class.report_gen_flag := 'Y';
    r_license_class.naic_class := null;
    r_license_class.process_type := 'ST_LICENSE_CLASS';
    r_license_class.loa_level_expire_ind := null;
    r_license_class.overwrite_order := null;
    r_license_class.display_mailing_address_ext := null;
    r_license_class.display_bus_address_ext := null;
    r_license_class.exam_level_ind := null;
    r_license_class.targets_by_loa_group_code := null;
    r_license_class.version_ol := 0;
    r_license_class.allow_filings := 'N';
    r_license_class.isactive := cur_lc.isactive;
​
    insert into license_class values r_license_class;
​
  end loop;
​
  p_migration_log ('END');
​
-- update TMP license_class_description
  update license_class
  set license_class_desc = 'Temp Producer'
  where license_class = 'TMP'
  and jurisdiction_id = c_state_id;
​
  if is_real_migration then
    commit;
  end if;
​
end;
