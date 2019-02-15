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

    private static function execStatement($query)
    {
        $statement = new \Cassandra\SimpleStatement($query);
        $session = app('db');
        $result = $session->execute($statement);
        return $result;
    }

    private static function newline($line) {
        echo $line . "<BR /><BR />";
    }

    private static function prepareSchemaAux()
    {
        // here preparing schema
    }s
}
