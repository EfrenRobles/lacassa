<?php

namespace Adrianheras\Lumencassandra;

use DB;

class PrepareSchema
{
    public static function prepareSchema()
    {
        self::newline("** Preparing Database Schema ** ");
        return self::prepareSchemaAux();
    }

    // PRIVATE METHODS

    private static function execStatement($query)
    {
        $statement = new \Cassandra\SimpleStatement($query);
        $session = app('db');
        $result = $session->execute($statement);
        return $result;
    }


    private static function prepareSchemaAux()
    {
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
              googleentityid varchar,
              createdDate timestamp            
            );
        ";

        $googleAdsAds_tableName = 'googleadsads';
        $q_googleadsads_schema = "
            CREATE TABLE IF NOT EXISTS {$googleAdsAds_tableName} (          
              id timeuuid primary key,
              customerid varchar,
              adgroupid varchar,
              title1 varchar,
              title2 varchar,
              title3 varchar,
              description varchar,
              description2 varchar,
              finalurl varchar,
              type varchar,
              status varchar,
              createdDate timestamp            
            );
        ";


        $googleAdsGroups_tableName = 'googleadsgroups';
        $q_googleadsads_schema = "
            CREATE TABLE IF NOT EXISTS {$googleAdsGroups_tableName} (          
              id timeuuid primary key,
              customerid varchar,
              campaignid varchar,
              name varchar,
              status varchar,
              type varchar,
              cpcbidmicros varchar,
              createdDate timestamp            
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

        self::newline("[OK] The keyspace exists");
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


        $result = self::execStatement($q_googleadsads_schema);
        self::newline("google ads groups table created");




    }

    private static function newline($line) {
        echo $line . "<BR /><BR />";
    }

}
