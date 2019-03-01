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
              monthlyBudget float,
              isAutomaticRenew boolean,
              locations set<frozen <location>>,,
              offering varchar,
              differing varchar,
              restInfo varchar,
              webpage varchar,
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
        $result = self::execStatement(
              "CREATE TYPE IF NOT EXISTS location ( 
                        city varchar, 
                        region varchar,
                        country varchar,
                        rest varchar
                      );"
        );

        if ($result->count() === 0) {
            throw new \Exception("Error in type creation");
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

    }

    private static function newline($line) {
        echo $line . "<BR /><BR />";
    }

}
