class SqlQueries:
    """
    SQL Helper class with set of SQLs used within Airflow tasks
    
    """
    
    # DROP TABLES SQLs
    db_schema_drop = ("""        
        DROP TABLE IF EXISTS public.fact_visit_event;
        DROP TABLE IF EXISTS public.staging_i94events;
        DROP TABLE IF EXISTS public.dim_visitor;
        DROP TABLE IF EXISTS public.dim_visa;
        DROP TABLE IF EXISTS public.dim_port;
        DROP TABLE IF EXISTS public.dim_location;
        DROP TABLE IF EXISTS public.dim_address;
        DROP TABLE IF EXISTS public.dim_mode;
        DROP TABLE IF EXISTS public.dim_time;
    """)
    
    # CREATE TABLES SQLs
    
    # Created ones if not exists
    create_all_talbes = ("""        
        CREATE TABLE IF NOT EXISTS public.dim_visitor(
            visitor_id int4 NOT NULL,
            age int4 NOT NULL,
            gender varchar(256),
            occup varchar(256),
            CONSTRAINT dim_visitor_pkey PRIMARY KEY (visitor_id)
        );

        CREATE TABLE IF NOT EXISTS public.dim_time(
            time_id int4 NOT NULL,
            time_year int4 NOT NULL,
            time_month int4 NOT NULL,
            time_day int4 NOT NULL,
            CONSTRAINT dim_time_pkey PRIMARY KEY(time_id)
        );

        CREATE TABLE IF NOT EXISTS public.fact_visit_event(            
            event_id varchar(32) NOT NULL PRIMARY KEY,
            visitor_id int4 NOT NULL REFERENCES public.dim_visitor(visitor_id) ,
            cit_id int4 NOT NULL REFERENCES public.dim_location(location_id),
            res_id int4 NOT NULL REFERENCES public.dim_location(location_id),
            port_code varchar(256) NOT NULL REFERENCES public.dim_port(port_code),
            address_code varchar(256) NOT NULL REFERENCES public.dim_address(address_code),
            mode_id int4 NOT NULL REFERENCES public.dim_mode(mode_id),
            visa_id int4 NOT NULL REFERENCES public.dim_visa(visa_id),
            visatype varchar(256),
            fltno varchar(256),
            airline_iata varchar(256), --REFERENCES public.dim_airline(iata_designator),
            arrdate_id int4 NOT NULL REFERENCES public.dim_time(time_id),
            depdate_id int4 NOT NULL REFERENCES public.dim_time(time_id)
        );
        
    """)
    
    
    # STAGING TABLES
    staging_i94events_table_create = ("""
        DROP TABLE IF EXISTS public.staging_i94events;
        CREATE TABLE IF NOT EXISTS public.staging_i94events(
            cicid int4,
            i94yr int4,
            i94mon int4,
            i94cit int4,
            i94res int4,
            i94port varchar(256),
            arrdate varchar(256),
            i94mode int4,
            i94addr varchar(256),
            depdate varchar(256),
            i94bir int4,
            i94visa int4,
            occup varchar(256),
            biryear int4,
            gender varchar(256),
            insnum varchar(256), -- do sprawdzenia
            airline varchar(256),
            admnum bigint,
            fltno varchar(256),
            visatype varchar(256)            
        );
    """)
    
        
    # LOOKUP DIMENSIONS TABLES
    # Created ones during
    mode_table_create = ("""
        DROP TABLE IF EXISTS public.dim_mode;
        CREATE TABLE IF NOT EXISTS public.dim_mode(
            mode_id int4 NOT NULL,
            mode_name varchar(256) NOT NULL,
            CONSTRAINT dim_mode_pkey PRIMARY KEY(mode_id)
        );
    """)

    
    location_table_create = ("""
        DROP TABLE IF EXISTS public.dim_location;
        CREATE TABLE IF NOT EXISTS public.dim_location(
            location_id int4 NOT NULL,
            location_name varchar(256) NOT NULL,
            CONSTRAINT dim_location_pkey PRIMARY KEY(location_id)
        );
    """)

    
    port_table_create = ("""
        DROP TABLE IF EXISTS public.dim_port;
        CREATE TABLE IF NOT EXISTS public.dim_port(
            port_code varchar(256) NOT NULL,
            port_name varchar(256) NOT NULL,
            state           varchar(10),
            info            varchar(256),
            ident           varchar(256),
            type            varchar(256),
            name            varchar(256),
            elevation_ft    int4,
            continent       varchar(256),
            iso_country     varchar(10),
            municipality    varchar(256),
            lon             real,
            lat             real,            
            CONSTRAINT dim_port_pkey PRIMARY KEY(port_code)
        );
    """)
    

    address_table_create = ("""
        DROP TABLE IF EXISTS public.dim_address;
        CREATE TABLE IF NOT EXISTS public.dim_address(
            address_code varchar(256) NOT NULL,
            address_name varchar(256) NOT NULL,
            CONSTRAINT dim_address_pkey PRIMARY KEY(address_code)
        );
    """)

    
    visa_table_create = ("""
        DROP TABLE IF EXISTS public.dim_visa;
        CREATE TABLE IF NOT EXISTS public.dim_visa(
            visa_id int4 NOT NULL,
            visa_category varchar(256) NOT NULL,
            CONSTRAINT  dim_visa_pkey PRIMARY KEY (visa_id)
        );    
    """)
        
    # LOOKUP DIMENSIONS TABLES
    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.dim_time(
            time_id int4 NOT NULL,
            time_year int4 NOT NULL,
            time_month int4 NOT NULL,
            time_day int4 NOT NULL,
            CONSTRAINT dim_time_pkey PRIMARY KEY(time_id)
        );        
    """)

    
    # SET OF SQLs used in Airflow Task to fill facts and dimensions tables
    dim_visitor_insert = ("""
        SELECT DISTINCT cicid as visitor_id, 
               i94bir as age,
               gender,
               occup
        FROM public.staging_i94events
        WHERE visitor_id NOT IN (
            SELECT visitor_id FROM dim_visitor
        )
    """)
    
    # oba selecty do utworzenia tabeli wymiaru czasu
    dim_arrdate_time_insert = ("""
        SELECT DISTINCT cast(arrdate as int4) as time_id, 
               cast(SUBSTRING(arrdate, 1, 4) as int4) as time_year, 
               cast(SUBSTRING(arrdate, 5, 2) as int4) as time_month,
               cast(SUBSTRING(arrdate, 7, 2) as int4) as time_day
        FROM public.staging_i94events
        WHERE time_id NOT IN (
            SELECT time_id FROM public.dim_time
        )
    """)
    
    dim_depdate_time_insert = ("""
        SELECT DISTINCT cast(depdate as int4) as time_id, 
               cast(SUBSTRING(depdate, 1, 4) as int4) as time_year, 
               cast(SUBSTRING(depdate, 5, 2) as int4) as time_month,
               cast(SUBSTRING(depdate, 7, 2) as int4) as time_day
        FROM public.staging_i94events
        WHERE time_id NOT IN (
            SELECT time_id FROM public.dim_time
        )
    """)
    

    fact_visit_insert = ("""
        SELECT 
               md5(cicid || arrdate) event_id,
               cicid as visitor_id,
               i94cit as cit_id,
               i94res as res_id,
               i94port as port_code,
               i94addr as address_code,
               i94mode as mode_id,
               i94visa as visa_id,
               visatype,
               fltno, 
               airline as airline_iata,
               cast(arrdate as int4) as arrdate_id,
               cast(depdate as int4) as depdate_id   
        FROM public.staging_i94events
    """)