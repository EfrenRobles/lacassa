<?php

namespace Adrianheras\Lumencassandra;

use DB;

class PrepareSchema
{
    /**
     * Preparing all DB model schemas
     *
     * PRE: field createddata (timestamp) should exists always
     *
     * @throws \Exception
     */
    public static function prepareSchema()
    {
        self::newline("** Preparing Database Schema ** ");

        // (1) Table creation

        $tableName = "campaigns";
        $q_campaigns_schema = "
            CREATE TABLE IF NOT EXISTS {$tableName} (          
              id timeuuid primary key,
              name varchar,
              target varchar,
              budget float,
              isAutomaticRenew boolean,
              locations set<frozen <location>>,,
              offering varchar,
              differing varchar,
              restInfo varchar,
              webpage varchar,
              googleentityid bigint,
              customerid bigint,
              status varchar,
              advertisingchanneltype varchar,                             
              createddate timestamp
            );
        ";

        $googleAdsAds_tableName = 'googleadsads';
        $q_googleadsads_schema = "
            CREATE TABLE IF NOT EXISTS {$googleAdsAds_tableName} (          
              id timeuuid primary key, 
              title1 varchar,
              title2 varchar,
              title3 varchar,
              description varchar,
              description2 varchar,
              finalurl varchar,
              type varchar,
              status varchar,
              customerid bigint,
              adgroupid varchar,              
              campaignid varchar,
              googleentityid bigint, 
              createddate timestamp            
            );
        ";


        $googleAdsGroups_tableName = 'googleadsgroups';
        $q_googleadsagroups_schema = "
            CREATE TABLE IF NOT EXISTS {$googleAdsGroups_tableName} (          
              id timeuuid primary key,              
              campaignid varchar,
              name varchar,
              status varchar,
              type varchar,
              cpcbidmicros varchar,
              createddate timestamp,
              googleentityid bigint,
              customerid bigint
                          
            );
        ";


        // Checking if keyspace exists
        $keyspace = env('DB_KEYSPACE', 'mykeyspace');
        $result = self::execStatement(
            "select * from system_schema.keyspaces where keyspace_name='{$keyspace}';"
        );

        if ($result->count() === 0) {
            throw new \Exception("The keyspace '{$keyspace}' does not exist");
        }

        self::newline("The keyspace exists");
        try {
            $result = self::execStatement(
                "CREATE TYPE IF NOT EXISTS location ( 
                            city varchar, 
                            region varchar,
                            country varchar,
                            rest varchar
                          );"
            );
        } catch (\Exception $e) {
            return self::newline('[ERROR] Error in type creation');
        }




        // Checking if the table "campaigns" exists
        $result = self::execStatement(
            "SELECT table_name
                   FROM system_schema.tables WHERE keyspace_name='" . $keyspace . "'
                     AND table_name='" . $tableName . "'");

        // Creating table if it does not exist
        if ($result->count() === 0) {
            echo "la tabla NO existía \n";
            $result = self::execStatement($q_campaigns_schema);
            self::newline("The table did not exist");
        } else {
            self::newline("The table existed");
        }


        $result = self::execStatement($q_googleadsads_schema);
        self::newline("google ads ads table created");


        $result = self::execStatement($q_googleadsagroups_schema);
        self::newline("google ads groups table created");


////////////////////////////////////////

        $tableName = "userlogins";
        $q_userlogins_schema = "
            CREATE TABLE IF NOT EXISTS {$tableName} (          
              id timeuuid ,              
              userid varchar,
              authtoken varchar primary key, 
              expiresat varchar,
              scopes varchar,
              createddate timestamp
            );
        ";

        $result = self::execStatement($q_userlogins_schema);
        self::newline("user logins table created");


        $tableName = "users";
        $q_users_schema = "
            CREATE TABLE IF NOT EXISTS {$tableName} (          
              userid int primary key,              
              first_name varchar, 
              last_name varchar,
              googleentityid bigint, 
              currencycode varchar, 
              datetimezone varchar, 
              createddate timestamp
            );
        ";

        $result = self::execStatement($q_users_schema);
        self::newline("user table created");


        $tableName = "companies";
        $q_companies_schema = "
            CREATE TABLE IF NOT EXISTS {$tableName} (          
              companyid int primary key,              
              active varchar, 
              address varchar, 
              google_ads_id varchar,
              googleentityid bigint,               
              industry_id varchar,              
              name varchar,              
              createddate timestamp
            );
        ";

        $result = self::execStatement($q_companies_schema);
        self::newline("companies table created");


        $tableName = "adgroupkeywords";
        $q_companies_schema = "
            CREATE TABLE IF NOT EXISTS {$tableName} (          
              id timeuuid primary key,   
              googleentityid bigint,   
              customerid bigint, 
              adgroupid varchar, 
              keyword varchar,              
              matchtype varchar,               
              createddate timestamp
            );
        ";

        $result = self::execStatement($q_companies_schema);
        self::newline("adgroupkeywordstable created");


        $tableSchemas = [
            [
                'name' => "campaigncriterions",
                'schema' => "
                        CREATE TABLE IF NOT EXISTS campaigncriterions (          
                          id timeuuid primary key,              
                          googleentityid bigint,
                          customerid bigint,
                          campaignid varchar,                            
                          radius varchar, 
                          streetaddress varchar, 
                          cityname varchar, 
                          postalcode varchar , 
                          countrycode varchar,   
                          microlatitude varchar,  
                          microlongitude varchar,               
                          createddate timestamp
                        );
                    "
            ],
            [
                'name' => "customerclients",
                'schema' => "
                        CREATE TABLE IF NOT EXISTS customerclients (          
                          googleentityid bigint primary key, 
                          userid timeuuid,                                          
                          customerid bigint,                                                
                          level varchar, 
                          hidden boolean,               
                          parentgoogleentityid bigint, 
                          parentgoogleentityname varchar,                            
                          createddate timestamp
                        );
                    "
            ],
            [
                // id timeuuid primary key,
                'name' => "metrics",
                'schema' => "
                        CREATE TABLE IF NOT EXISTS metrics (                              
                          metricid varchar primary key, 
                          googleentityid bigint,                                                                   
                          customerid bigint,                                                 
                          source varchar, 
                          metric varchar, 
                          value varchar, 
                          date varchar, 
                          campaignid varchar,                                                    
                          createddate timestamp
                        );
                    "
            ]
        ];


        foreach ($tableSchemas as $tableSchema){
            $result = self::execStatement($tableSchema['schema']);
            self::newline("{$tableSchema['schema']} created");
        }

    }


    // PRIVATE METHODS


    private static function execStatement($query)
    {
        $statement = new \Cassandra\SimpleStatement($query);
        $session = app('db');
        $result = $session->execute($statement);
        return $result;
    }


    private static function newline($line)
    {
        print("\n[*] {$line}\n");
    }

}
